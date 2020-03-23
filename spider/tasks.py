#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-06 17:09
import csv
import os
import time
import ujson
import asyncio

from django.conf import settings

from spider import ws_crawl
from exspider.utils.redis_con import AsyncRedis, REDIS_CON

loop = asyncio.get_event_loop()

redis_conn = AsyncRedis(loop)


def stop_spider(exchange_id=None, symbol=None, ws_type=settings.BASE_WS_TYPE_TRADE):
    """
    功能:
        停止爬虫
    参数:
        exchange_id: 如果不传, 则停止所有, 否则停止指定交易所
        symbol: 不传 停止交易所, 否则只停止交易对
    """
    print(f'停止 spider {exchange_id} {symbol}')
    if not exchange_id:
        loop.run_until_complete(redis_conn.cmd_stop_exchange(exchange_id, symbol, ws_type=ws_type))
        return f'停止 所有启动中的爬虫完成!'
    else:
        loop.run_until_complete(redis_conn.cmd_stop_exchange(exchange_id, symbol, ws_type=ws_type))
        return f'{exchange_id} {symbol} 执行停止!'


def start_spider(exchange_id=None, symbol=None, ws_type=settings.BASE_WS_TYPE_TRADE):
    """
    功能:
        设置交易所 启动状态
    参数:
        exchange_id: 不传 启动全部, 否则只启动指定交易所
        symbol: 不传, 启动指定交易所, 否则只发送symbol信号
    """
    print(f'开启 spider {exchange_id} {symbol}')
    if not exchange_id:
        print('启动 所有交易所')
        loop.run_until_complete(redis_conn.cmd_start_exchange(exchange_id, symbol, ws_type=ws_type))
        return
    else:
        loop.run_until_complete(redis_conn.cmd_start_exchange(exchange_id, symbol, ws_type=ws_type))
        print(f'{exchange_id} {symbol} 设置成功等待启动...')


def parse_exchange_trade_csv(exchange_id_str=None, symbol_str=None, is_forever=False, ws_type=settings.BASE_WS_TYPE_TRADE):
    """
    功能:
        生成csv 文件
        每个ex 一个task 采取 异步的方式
    步骤:
        1. 获取 所有 exchange

        2. 获取 ex的所有symbol
    """
    if not exchange_id_str:
        exchange_ids = [x.decode() for x in REDIS_CON.hkeys(settings.EXCHANGE_STATUS_KEY_MAP[ws_type])]
    elif ',' in exchange_id_str:
        exchange_ids = exchange_id_str.split(',')
    else:
        exchange_ids = [exchange_id_str]
    if not symbol_str:
        symbols = None
    elif ',' in symbol_str:
        symbols = symbol_str.split(',')
    else:
        symbols = [symbol_str]
    for exchange_id in exchange_ids:
        loop.run_until_complete(save_exchange_trade_to_csv(exchange_id, symbols, ws_type=ws_type))
    if is_forever:
        tasks = [loop.create_task(get_pending_exchange_to_csv_forever(ws_type=ws_type)) for x in range(5)]
        asyncio.gather(*tasks)
        loop.run_forever()


async def get_pending_exchange_to_csv_forever(ws_type=settings.BASE_WS_TYPE_TRADE):
    """
    功能:
        持续等待 需要生成csv的 symbol:exchange
    示例:
        aicoin 的str @: 'aicoin/btcusdt:huobipro'
        其他 @: 'btcusdt:huobipro'
    """
    cur = await redis_conn.redis
    key = settings.PENDING_CSV_EXCHANGE_KEY_MAP[ws_type]
    while 1:
        try:
            symbol_ex_str = await cur.lpop(key, encoding='utf-8')
            if symbol_ex_str:
                if '/' in symbol_ex_str:
                    exchange_id, symbol = symbol_ex_str.split('/')
                else:
                    symbol, exchange_id = symbol_ex_str.split(':')
                await save_exchange_trade_to_csv(exchange_id, [symbol], ws_type=ws_type)
        except:
            ...
        await asyncio.sleep(0)


async def save_exchange_trade_to_csv(exchange_id, symbols=None, ws_type=settings.BASE_WS_TYPE_TRADE):
    """
    协程:
        生成一个交易所的 csv
    """
    now_tms = int(time.time())
    cur = await redis_conn.redis
    symbol_key = settings.SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
    if not symbols:
        if exchange_id in ws_crawl.future_exchanges:
            ex = getattr(ws_crawl, exchange_id)(loop=loop)
            all_symbols = [x for x in ex.symbols.values()]
        else:
            all_symbols = [x for x in await cur.hkeys(symbol_key, encoding='utf-8')]
    else:
        all_symbols = symbols
    for symbol in all_symbols:
        if exchange_id == settings.BASE_AI_COIN_EXCHANGE_ID:
            sbl, ex_id = symbol.split(':')
            csv_file = settings.CSV_PATH_MAP[ws_type][exchange_id].format(exchange_id=ex_id, symbol=sbl, tms=now_tms)
        else:
            csv_file = settings.CSV_PATH_MAP[ws_type]['exchange'].format(exchange_id=exchange_id, symbol=symbol, tms=now_tms)

        data_key = settings.EXCHANGE_SYMBOL_CACHE_KEY_MAP[ws_type].format(exchange_id, symbol)
        rows = []
        for x in range(settings.ONE_TIME_SAVE_COUNT):
            try:
                trade = await cur.lpop(data_key, encoding='utf-8')
            except:
                trade = None
            if not trade:
                break
            rows.append(ujson.loads(trade))
        if rows:
            save_ret = await save_data_2_csv(csv_file, rows)
            if not save_ret:
                values = [ujson.dumps(x) for x in rows]
                await cur.lpush(data_key, *values)


async def save_data_2_csv(csv_file, rows):
    try:
        dir_path = os.path.split(csv_file)[0]
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        # 查看当前目录最后一个文件, 并根据文件大小判断是否生成下一条
        base_tms = 0
        try:
            base_tms = max([int(file.replace('.csv', '')) for file in os.listdir(dir_path)])
            is_append = True
        except:
            is_append = False

        last_file = csv_file
        if is_append:
            _last_file = f'{dir_path}/{base_tms}.csv'
            if await get_file_size(_last_file) < settings.CSV_SIZE:
                last_file = _last_file
        with open(last_file, 'a+', newline='') as f:
            f_csv = csv.writer(f)
            f_csv.writerows(rows)
        if is_append:
            print(f'成功追加: {last_file}')
        else:
            print(f'成功生成: {last_file}')
        return True
    except Exception as e:
        return False


async def get_file_size(file_path):
    """
    功能:
        获取文件大小 单位 MB
    """
    size = os.path.getsize(file_path)
    size = size / float(1024*1024)
    return round(size, 2)
