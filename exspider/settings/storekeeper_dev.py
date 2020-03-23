#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-04 15:20
from .base import *

IS_DEBUG = True

PROXY = None

# redis settings
LOCAL_REDIS_CONF = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0,
}


TRADES_DB_CONF = {
    "user": "postgres",
    "password": "",
    "host": "",
    "port": "5432"
}

OHLCV_DB_CONF = TRADES_DB_CONF


STOPKEEPER_REDIS_CONFIG = LOCAL_REDIS_CONF

