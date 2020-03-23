#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-04 19:30

import sys

from django.conf import settings

from exspider.utils.pg_con import TradePg


async def handle_csv_line(line):
    """
    功能:
        处理成 入库格式
        [
            timestamp int,
            trade_id str,
            direction str,
            price float,
            amount float
        ]
    """
    line = line.strip()
    if not line:
        return
    if 'price' in line:
        return
    if 'sell' in line:
        trade_str = line.replace('sell', 's')
    elif 'ask' in line:
        trade_str = line.replace('ask', 's')
    elif 'bid' in line:
        trade_str = line.replace('bid', 'b')
    elif 'buy' in line:
        trade_str = line.replace('buy', 'b')
    else:
        trade_str = line
    trade = trade_str.split(',')
    return [
        int(trade[0]),
        trade[1],
        trade[2],
        float(trade[3]),
        float(trade[4]),
    ]


async def go_insert(exchange_id, symbol):
    """
    功能:
        读取 管道中的trade 数据 保存到 pg中
    """
    if not (exchange_id and symbol):
        print('传参错误!')
        return
    trade_list = []
    for line in sys.stdin:
        trade = await handle_csv_line(line)
        if not trade:
            continue
        trade_list.append(trade)
        if len(trade_list) >= settings.ONE_TIME_SAVE_COUNT:
            new_trade_list = trade_list
            trade_list = []
            try:
                async with TradePg(exchange_id) as pg:
                    await pg.insert_many(symbol, new_trade_list)
            except:
                trade_list.extend(new_trade_list)
    else:
        try:
            async with TradePg(exchange_id) as pg:
                await pg.insert_many(symbol, trade_list)
        except Exception as e:
            print(e)
