#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-09 10:19

import asyncio

from django.core.management import BaseCommand

from storekeeper.ohlcv import OHLCV

loop = asyncio.get_event_loop()

class Command(BaseCommand):

    """
    CMD:
        python manage.py start_ohlcv
    """

    def handle(self, *args, **options):
        ohlcv = OHLCV(loop)
        loop.create_task(ohlcv.start_care_user_sub_pair())
        loop.create_task(ohlcv.start_get_ex_trades_forever())
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            loop.close()
            exit(0)

