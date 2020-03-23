#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-18 14:55

import asyncio
from django.core.management import BaseCommand
from storekeeper.utils import go_insert_ohlcv


loop = asyncio.get_event_loop()


class Command(BaseCommand):

    """
    CMD:
        python manage.py import_trade huobipro btcusdt
    """

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)

    def handle(self, *args, **options):
        """
        功能:
            1. 建库
            2. 建表
            3. 插入
        """
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        loop.run_until_complete(go_insert_ohlcv(exchange_id, symbol))
