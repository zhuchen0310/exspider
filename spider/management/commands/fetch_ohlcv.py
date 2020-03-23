#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:59

import asyncio
import ujson
import time
import datetime

from django.core.management import BaseCommand
from django.conf import settings

from spider import ws_crawl
from exspider.utils import redis_con as redis
from exspider.utils.enum_con import TimeFrame
from spider.tasks import save_exchange_trade_to_csv
from exspider.common.funcs import get_spider_host_map
proxy = settings.PROXY

redis_conn = redis.REDIS_CON
loop = asyncio.get_event_loop()

class Command(BaseCommand):

    """
    CMD:
        python manage.py fetch_ohlcv spider1
    """
    this_spider_symbols = {}
    exchange_symbol_last_kline_cache = {}
    request_sleep_time = 60

    def add_arguments(self, parser):
        parser.add_argument('spider_name', type=str)

    def handle(self, *args, **options):
        spider_name = options['spider_name']
        spider_map = get_spider_host_map()
        all_exchanges = ws_crawl.exchanges
        tasks = []
        for exchange_id in all_exchanges:
            print(f'Start {exchange_id}...')
            for x in range(5):
                try:
                    # 直到 成功创建对象
                    ex = getattr(ws_crawl, exchange_id)(loop=loop, http_proxy=proxy, ws_proxy=proxy, ws_type=ws_crawl.WS_TYPE_KLINE)
                    break
                except:
                    time.sleep(2)
            else:
                print(f'{exchange_id} init error!')
                continue
            if not ex.symbols:
                print(ex.exchange_id)
                continue
            if exchange_id in ws_crawl.future_exchanges:
                exchange_symbols = sorted(ex.symbols.values())
            else:
                exchange_symbols = sorted(ex.symbols)
            spider_bind_count = len(exchange_symbols) // len(spider_map)
            for i, _spider_name in enumerate(spider_map):
                start = i * spider_bind_count + 1
                if i == len(spider_map) - 1:
                    end = len(exchange_symbols)
                else:
                    end = start + spider_bind_count
                spider_key = settings.SPIDER_EXCHANGE_SYMBOLS_KEY.format(_spider_name)
                spider_symbols = exchange_symbols[start: end]
                if _spider_name == spider_name:
                    self.this_spider_symbols[exchange_id] = spider_symbols
                redis_conn.hset(spider_key, exchange_id, ujson.dumps(spider_symbols))
            else:
                print(f'设置完: {exchange_id}, 当前Spider 绑定了 {len(self.this_spider_symbols[exchange_id])}个')
                time.sleep(2)
                tasks.append(asyncio.ensure_future(self.go_kline_forever(ex, spider_name=spider_name)))
                asyncio.gather(ex.publisher.connect())
        print(f'tasks 数量: {len(tasks)}')
        time.sleep(5)
        loop.run_forever()

    async def go_kline_forever(self, ex, spider_name):
        """
        功能:
            一个交易所一个task, 处理task
        """
        exchange_id = ex.exchange_id
        symbols = self.this_spider_symbols[exchange_id]
        start_hour = datetime.datetime.now().hour
        while True:
            # 合约 每小时 刷新一次
            now_hour = datetime.datetime.now().hour
            if now_hour != start_hour:
                start_hour = now_hour
                if await self.init_symbol(ex, spider_name):
                    symbols = self.this_spider_symbols[exchange_id]
            for symbol in symbols:
                try:
                    kline_list = await ex.get_restful_klines(symbol, TimeFrame.m1.value)
                    if kline_list:
                        await ex.logger.debug(f'Fetch: {exchange_id} {symbol} 长度: {len(kline_list)}')
                        kline_list = kline_list[:-1]
                        pair_ex = f'{symbol}:{exchange_id}'

                        if pair_ex in self.exchange_symbol_last_kline_cache:
                            kline_list = [x for x in kline_list if
                                          x[0] > self.exchange_symbol_last_kline_cache[pair_ex]]
                        if kline_list:
                            self.exchange_symbol_last_kline_cache[pair_ex] = kline_list[-1][0]
                            await ex.redis_con.add_exchange_symbol_klines(exchange_id, symbol, kline_list)
                except Exception as e:
                    await ex.logger.error(f'{exchange_id} {symbol}: {e}')
                finally:
                    await asyncio.sleep(1)
            else:
                await ex.logger.info(f'{exchange_id} 轮训完一次, 暂停{self.request_sleep_time}秒!')
                await save_exchange_trade_to_csv(exchange_id, symbols=symbols, ws_type=ws_crawl.WS_TYPE_KLINE)
                await asyncio.sleep(self.request_sleep_time)

    async def init_symbol(self, ex, spider_name):
        spider_map = get_spider_host_map()
        exchange_id = ex.exchange_id
        try:
            ex.symbols = ex.get_symbols()
            exchange_symbols = sorted(ex.symbols.values())
            spider_bind_count = len(exchange_symbols) // len(spider_map)
            for i, _spider_name in enumerate(spider_map):
                start = i * spider_bind_count + 1
                if i == len(spider_map) - 1:
                    end = len(exchange_symbols)
                else:
                    end = start + spider_bind_count
                spider_key = settings.SPIDER_EXCHANGE_SYMBOLS_KEY.format(_spider_name)
                spider_symbols = exchange_symbols[start: end]
                if _spider_name == spider_name:
                    self.this_spider_symbols[exchange_id] = spider_symbols
                    redis_conn.hset(spider_key, exchange_id, ujson.dumps(spider_symbols))
                else:
                    print(f'更新: 设置完: {exchange_id}, 当前Spider 绑定了 {len(self.this_spider_symbols[exchange_id])}个')
            return True
        except Exception as e:
            await ex.logger.error(e)
            return False