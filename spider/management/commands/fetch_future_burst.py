#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-07-10 15:24

import asyncio
import time
import datetime
import hashlib
import ujson

from django.core.management import BaseCommand
from django.conf import settings

from spider import ws_crawl
from spider.tasks import save_data_2_csv
from exspider.utils.logger_con import get_logger
from exspider.utils.redis_con import AsyncRedis

logger = get_logger()
loop = asyncio.get_event_loop()

BURST_LAST_SAVE_KEY = 'hash:burst_last_save_cache'

class Command(BaseCommand):

    """
    CMD:
        python manage.py fetch_future_burst okex_future
    功能:
        循环抓取 爆仓记录
    """
    redis = AsyncRedis(loop=loop)

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)

    def handle(self, *args, **options):
        exchange_id = options['exchange_id']
        loop.create_task(self.go(exchange_id))
        loop.run_forever()

    async def go(self, exchange_id):
        cur = await self.redis.redis
        ex = getattr(ws_crawl, exchange_id)()
        last_hour = datetime.datetime.now().hour
        while True:
            now_hour = datetime.datetime.now().hour
            if now_hour != last_hour:
                await logger.info('初始化symbol')
                last_hour = now_hour
                ex.symbols = ex.get_symbols()
            # ex.symbols = {'BTC-USD-190712': ex.symbols['BTC-USD-190712']}
            for symbol in ex.symbols:
                await asyncio.sleep(0.3)
                await logger.info(f'start {symbol}')
                bursts = await ex.get_restful_bursts(symbol)
                if not bursts:
                    continue
                last_save_data = await cur.hget(BURST_LAST_SAVE_KEY, ex.symbols[symbol])
                if not last_save_data:
                    await cur.hset(BURST_LAST_SAVE_KEY, ex.symbols[symbol], ujson.dumps({
                        'tms': bursts[-1][0],
                        'hash': hashlib.md5(f'{bursts[-1]}'.encode()).hexdigest()
                    }))
                else:
                    last_burst = ujson.loads(last_save_data)
                    for index, burst in enumerate(bursts):
                        if burst[0] == last_burst['tms'] and hashlib.md5(f'{burst}'.encode()).hexdigest() == last_burst['hash']:
                            bursts = bursts[index+1:]
                            break
                    if not bursts:
                        continue
                    await cur.hset(BURST_LAST_SAVE_KEY, ex.symbols[symbol], ujson.dumps({
                        'tms': bursts[-1][0],
                        'hash': hashlib.md5(f'{bursts[-1]}'.encode()).hexdigest()
                    }))
                # TODO save csv '/opt/db/{exchange_id}/{symbol}/burst_csv/{tms}.csv'
                now_tms = int(time.time())
                csv_file = settings.EXCHANGE_BURST_SYMBOL_CSV_PATH.format(exchange_id=exchange_id, symbol=ex.symbols[symbol], tms=now_tms)
                await save_data_2_csv(csv_file, bursts)
