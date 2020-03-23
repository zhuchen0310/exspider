#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:59

import asyncio
import time
import ujson

from django.core.management import BaseCommand
from django.conf import settings

from spider import ws_crawl
from exspider.utils.enum_con import SpiderStatus
from exspider.utils import redis_con as redis

redis_conn = redis.REDIS_CON

loop = asyncio.get_event_loop()
proxy = settings.PROXY


class Command(BaseCommand):

    """
    CMD:
        python manage.py care_aicoin_spider kline
    说明:
        监控 aicoin 交易所运行状态
    思路:
        开始指令 >>
        结束指令 << STOP
    """
    exchange_id = 'aicoin'

    def add_arguments(self, parser):
        parser.add_argument('ws_type', type=str)

    def handle(self, *args, **options):
        ws_type = options['ws_type']
        if ws_type not in settings.EXCHANGE_STATUS_KEY_MAP:
            print(f'{ws_type} is Error!')
        self.go_ws(ws_type)

    def go_ws(self, ws_type):
        exchange_id = self.exchange_id
        ex_key = settings.EXCHANGE_STATUS_KEY_MAP[ws_type]['trade']
        ex_data = redis_conn.hget(ex_key, exchange_id)
        if not ex_data:
            print(f'交易所 {exchange_id} 未添加...')
            return
        elif int(ex_data) == SpiderStatus.running.value:
            print(f'交易所 {exchange_id} 正在运行中...')
        else:
            redis_conn.hset(ex_key, exchange_id, SpiderStatus.running.value)
            print(f'交易所 {exchange_id} 开始启动...')
        ex = getattr(ws_crawl, exchange_id)(loop=loop, http_proxy=proxy, ws_proxy=proxy)
        try:
            loop.run_until_complete(ex.start_crawl())
            ex.close_spider()
        except KeyboardInterrupt:
            ex.close_spider()
            exit(0)