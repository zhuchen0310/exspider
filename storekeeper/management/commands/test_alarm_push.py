#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 10:57
import asyncio

from django.core.management import BaseCommand

from exspider.utils.amqp_con import AioPikaConsumer
from storekeeper.alarm.test_push import PairAlarm
from exspider.common.funcs import get_spider_host_map

loop = asyncio.get_event_loop()


class Command(BaseCommand):

    help = 'python manage.py test_alarm_push spider2 market.kline.binance.uuu.btc.*.*.*  alarm_test_111 storekeeper_test'

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
        except KeyboardInterrupt:
            loop.close()