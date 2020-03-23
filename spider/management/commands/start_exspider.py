#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:59

import asyncio
import ujson
import time

from django.core.management import BaseCommand
from django.conf import settings

from spider import ws_crawl
from exspider.utils.enum_con import SpiderStatus
from exspider.utils import redis_con as redis
from exspider.utils.amqp_con import AioPikaPublisher
proxy = settings.PROXY

redis_conn = redis.REDIS_CON
loop = asyncio.get_event_loop()

PYTHONTRACEMALLOC = 1
import tracemalloc

tracemalloc.start()

class Command(BaseCommand):

    """
    CMD:
        python manage.py start_exspider huobipro kline
    说明:
        暂不支持.. 不支持多个交易所启动
    """

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('ws_type', type=str)


    def handle(self, *args, **options):

        exchange_id_str = options['exchange_id']
        ws_type = options['ws_type']
        if ws_type not in settings.EXCHANGE_STATUS_KEY_MAP:
            print(f'{ws_type} Error!')
            return
        ex_key = settings.EXCHANGE_STATUS_KEY_MAP[ws_type]
        if ',' in exchange_id_str:
            exchange_ids = exchange_id_str.split(',')
        else:
            exchange_ids = [exchange_id_str]

        for ex_id in exchange_ids:
            ex = getattr(ws_crawl, ex_id)(loop=loop, http_proxy=proxy, ws_proxy=proxy, ws_type=ws_type)
            ex_symbols_key = settings.SYMBOL_STATUS_KEY_MAP[ws_type].format(ex_id)
            symbols_data = redis_conn.hgetall(ex_symbols_key)
            symbols_map = {x.decode(): ujson.loads(symbols_data[x]) for x in symbols_data}

            redis_conn.hset(ex_key, ex_id, SpiderStatus.running.value)
            print(f'交易所 {ex_id} {ws_type} 开始启动...')
            redis_conn.delete(settings.SPIDER_STATUS_KEY_MAP[ws_type].format(ex_id))
            # 把 下架的交易对删除掉
            delete_symbols = [x for x in symbols_map if x not in ex.symbols]
            for s in delete_symbols:
                print(f'删除{s}')
                redis_conn.hdel(ex_symbols_key, s)
            # 添加交易对 初始状态
            pending_symbols = [x for x in symbols_map if symbols_map[x][redis.STATUS_KEY] == SpiderStatus.running.value
                               and not symbols_map[x][redis.PID_KEY]]
            add_symbols = [x for x in ex.symbols if x not in symbols_map]
            add_symbols_map = {
                x: ujson.dumps({
                    redis.TMS_KEY: int(time.time()),
                    redis.STATUS_KEY: SpiderStatus.running.value,
                    redis.PID_KEY: ''})
                for x in add_symbols
            }
            if add_symbols_map:
                redis_conn.hmset(ex_symbols_key, add_symbols_map)
                pending_symbols.extend(add_symbols)
            symbol_limit = ex.max_sub_num
            pending_symbols = sorted(pending_symbols)
            symbol_list = [pending_symbols[i:i + symbol_limit] for i in range(0, len(pending_symbols), symbol_limit)]
            publisher = None
            try:
                for symbols in symbol_list:
                    while 1:
                        try:
                            time.sleep(2)
                            # 知道成功创建对象
                            _ex = getattr(ws_crawl, ex_id)(loop=loop, http_proxy=proxy, ws_proxy=proxy, publisher=publisher, ws_type=ws_type)
                            break
                        except:
                            continue
                    loop.create_task(_ex.start_crawl(ws_type, symbols))
                loop.run_forever()
                ex.close_spider(ws_type=ws_type)
            except KeyboardInterrupt:
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics('lineno')

                print("[ Top 10 ]")
                for stat in top_stats[:10]:
                    print(stat)
                if ex.close_spider(ws_type=ws_type):
                    exit(0)
