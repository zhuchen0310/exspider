#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 10:57
import asyncio
import ujson

from django.core.management import BaseCommand
from django.conf import settings

from exspider.utils.redis_con import REDIS_CON as redis_conn
from storekeeper.alarm.alarm_pair_price import PairAlarm

loop = asyncio.get_event_loop()


class Command(BaseCommand):
    help = 'python manage.py test_price_update huobipro btc_usdt 5000 local_dev'

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)
        parser.add_argument('limit', type=str)

    def handle(self, *args, **options):
        print('Start...')
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        limit = options['limit']
        try:
            loop.run_until_complete(self.start(exchange_id, symbol, limit))
        except KeyboardInterrupt:
            loop.close()

    def get_ohlcv(self, exchange_id, symbol, limit):
        import requests
        url = f"{settings.MARKETS_SERVER_RESTFUL_URL}/v2/coin/ohlcv"

        querystring = {"exchange":exchange_id,"symbol":symbol,"step":"1min","type":"0","limit":limit}

        payload = ""
        headers = {
            'cache-control': "no-cache",
            'Postman-Token': "c8011f70-0567-4135-aa25-309d29a26197"
            }

        response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
        return response.json()['data']['list']

    async def start(self, exchange_id, symbol, limit):
        kline_list = self.get_ohlcv(exchange_id, symbol, limit)
        print(len(kline_list))
        base, quote = symbol.split('_')
        redis_conn.hset(settings.COIN_PUSH_FROM_PAIR_KEY, base, f'{symbol.replace("_", "")}:{exchange_id}')
        routing_key = f"market.kline.{exchange_id}.{base}.{quote}.spot.0.0"
        alarm = PairAlarm(loop=loop, is_test=True)
        for kline in kline_list:
            kline = await self.format_ohlcv(kline)
            data = {
                'c': 'kline',  # channel
                'e': exchange_id,  # 交易所id
                't': kline[0],  # 时间戳
                's': symbol.replace('_', ''),  # 交易对
                'd': kline
            }
            await alarm.pair_alarm_msg_process_func(routing_key=routing_key, data=ujson.dumps(data))

    async def format_ohlcv(self, kline):
        kline[1] = float(kline[1])
        kline[2] = float(kline[2])
        kline[3] = float(kline[3])
        kline[4] = float(kline[4])
        kline[5] = float(kline[5])
        return kline
