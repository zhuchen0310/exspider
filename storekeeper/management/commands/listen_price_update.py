#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 10:57
import asyncio
import time

from django.core.management import BaseCommand
from django.conf import settings

from exspider.utils.amqp_con import AioPikaConsumer, ConsumError
from exspider.utils.email_con import send_email
from storekeeper.alarm.alarm_pair_price import PairAlarm
from exspider.common.funcs import get_spider_host_map

loop = asyncio.get_event_loop()


class Command(BaseCommand):

    help = 'python manage.py listen_price_update spider2 market.kline.binance.btc.usdt.*.*.*  alarm_test_1 storekeeper_test'

    def add_arguments(self, parser):
        parser.add_argument('spider_name', type=str)
        parser.add_argument('routing_keys', type=str)
        parser.add_argument('queue_name', type=str)

    def handle(self, *args, **options):
        print('Start...')
        _spider_name = options['spider_name']
        _queue_name = options['queue_name']
        _routing_keys = options['routing_keys']
        alarm = PairAlarm(loop=loop)
        host = get_spider_host_map()[_spider_name]
        _mq_url = f'amqp://guest:guest@{host}:5672/'
        consumer = AioPikaConsumer(
            _mq_url,
            loop,
            msg_process_func=alarm.pair_alarm_msg_process_func,
            routing_keys=_routing_keys,
            queue_name=_queue_name)
        try:
            loop.run_until_complete(consumer.start())
        except ConsumError:
            if settings.APP_TYPE == 'WWW':
                send_email(settings.RECIVER_EMAIL_LIST, title='MQ 消费者断开', body=f'{_spider_name}|{_queue_name}|{_routing_keys} 无法消费!')
            time.sleep(10)
            print('Stop...')
            exit(0)
        except KeyboardInterrupt:
            loop.close()
            exit(0)
