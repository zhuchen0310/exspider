#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-27 17:59

import asyncio

from django.core.management import BaseCommand
from django.conf import settings

from spider import ws_crawl
from spider.tasks import start_spider


proxy = settings.PROXY
loop = asyncio.get_event_loop()


class Command(BaseCommand):

    """
    CMD:
        python manage.py init_exchange_symbols huobipro
    说明:
        暂不支持.. 不支持多个交易所启动
    """

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)


    def handle(self, *args, **options):
        exchange_id = options['exchange_id']
        if ',' in exchange_id:
            exchange_ids = exchange_id.split(',')
        elif exchange_id == 'all':
            exchange_ids = ws_crawl.exchanges
        else:
            exchange_ids = [exchange_id]
        for ex_id in exchange_ids:
            for ws_type in settings.EXCHANGE_STATUS_KEY_MAP:
                print(ex_id, ws_type)
                ex = getattr(ws_crawl, ex_id)(loop=loop, http_proxy=proxy, ws_proxy=proxy)
                try:
                    symbols = ex.symbols
                    for symbol in symbols:
                        start_spider(ex_id, symbol, ws_type=ws_type)
                except KeyboardInterrupt:
                    exit(0)
