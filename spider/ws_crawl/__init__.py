#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 16:34
from django.conf import settings
WS_TYPE_TRADE = settings.BASE_WS_TYPE_TRADE
WS_TYPE_KLINE = settings.BASE_WS_TYPE_KLINE

from._base import HoldBase, WsError
from .aicoin import aicoin

from .huobipro import huobipro
from .binance import binance
from .bitmex import bitmex
from .hotbit import hotbit
from .okex import okex
from .bitfinex import bitfinex
from .gateio import gateio
from .bibox import bibox
from .coinbasepro import coinbasepro
from .bittrex import bittrex
from .kucoin import kucoin
from .hitbtc import hitbtc
from .bitstamp import bitstamp
from .bithumb import bithumb
from .citex import citex
from .kraken import kraken
from .mxc import mxc
from .idax import idax
from .biki import biki
from .zb import zb

from .okex_future import okex_future

exchanges = [
    # 'aicoin',

    'huobipro',
    'binance',
    'bitmex',
    'hotbit',
    'okex',
    'bitfinex',
    'gateio',
    'bibox',
    'coinbasepro',
    'bittrex',
    'kucoin',
    'hitbtc',
    'bitstamp',
    'bithumb',
    'citex',
    'kraken',
    'mxc',
    'idax',
    'biki',
    'zb',

    'okex_future',

]

future_exchanges = [
    'okex_future',
]


__all__ = ('exchanges', 'WS_TYPE_TRADE', 'WS_TYPE_KLINE', 'future_exchanges')