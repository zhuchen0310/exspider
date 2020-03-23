#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-12 16:31
import asyncio
from storekeeper.celery import app
from storekeeper.ohlcv import OHLCV
loop = asyncio.get_event_loop()
ohlcv = OHLCV(loop)


@app.task
def add(x=1, y=1):
    return x + y

@app.task
def start_ohlcv():
    loop.run_until_complete(ohlcv.start_ohlcv())