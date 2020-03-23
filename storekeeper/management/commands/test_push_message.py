#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 10:57
import asyncio
import time
from django.core.management import BaseCommand

from exspider.utils.enum_con import PushType
from storekeeper.alarm.base import get_exchange_pair_data
from django.conf import settings
from storekeeper.push_center import push_main

loop = asyncio.get_event_loop()


class Command(BaseCommand):
    help = 'python manage.py test_push_message huobipro btc_usdt '' 1 local_dev'

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)
        parser.add_argument('message', type=str)
        parser.add_argument('is_push', type=bool)

    def handle(self, *args, **options):
        print('Start...')
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        message = options['message']
        is_push = 1 if options['message'] else 0
        try:
            base_name, quote_name = symbol.split('_')
            loop.run_until_complete(self.go(exchange_id, base_name, quote_name, message, is_push))
        except KeyboardInterrupt:
            loop.close()

    async def go(self, exchange_id, base_name, quote_name, message, is_push):
        ret_data = await get_exchange_pair_data(exchange_id, base_name, quote_name, loop=loop)
        title = f'{base_name.upper()}test'
        if not message:
            message = '【短时下跌】据币安行情，BTC过去4分钟下跌1.04%，下跌金额为73.45USDT，现报7096.0。5月17日19:43'
        push_data = {
            'uids': [],
            'title': title,
            'coin_id': ret_data['coin_id'],
            'body': message,
            'summary': message,
            'jump': settings.PAIR_JUMP_URL.format(
                symbol=f'{base_name}-{quote_name}',
                exchange_id=exchange_id,
                pair_id=ret_data['pair_id'],
                hasOhlcv=ret_data['has_ohlcv'],
                coin_id=ret_data['coin_id'],
            ),
            'timestamp': int(time.time()),
            'type': 'PairAlarm',
            'is_push': is_push,
            'message_push_type': 'remind',
        }
        call_php = {
            'call_func': settings.PHP_NOTICE_METHOD['pair_alarm'],
            'data': push_data
        }
        call_platform = {
            'call_func': '',
            'data': {
                'now_price': 10008.88,
                'is_rise': True,
                'pass_price': 10000,
                'coin_id': ret_data['coin_id'],
                'minutes_ago': 5,
                'pct': '1.5',
                'rise_str_2': '涨',
            }
        }
        push_type = PushType.pass_price.value
        # push_type = PushType.rise_or_fall.value
        await push_main(
            base_name=base_name, quote_name=quote_name, exchange_id=exchange_id,
            call_php=call_php, call_platform=call_platform, push_type=push_type)
        print(title, message)
