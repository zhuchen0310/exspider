#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-13 16:11

from enum import Enum, unique


class WsType(Enum):
    """
    说明:
        websocket 数据源和消息类型枚举值
        client >> 客户端
        spider >> 爬虫
        store >> 数据中心

        sub >> 订阅
        unsub >> 取消订阅
        pub >> 发布
    """

    CLIENT = 'client'
    SPIDER = 'spider'
    STORE = 'store'

    SUB = 'sub'
    UNSUB = 'unsub'
    PUB = 'pub'


class SpiderStatus(Enum):
    pending = 1
    running = 2
    error_stopped = 3
    stopped = 4


class TimeFrame(Enum):
    """
    说明:
        timeframe 枚举
    用法:
        TimeFrame.m1.value -> '1min'
        TimeFrame.m1.name -> 'm1'

        TimeFrame['m1'].value -> '1min'
        TimeFrame['m1'].name -> 'm1'

        TimeFrame('1min').value -> '1min'
        TimeFrame('1min').name -> 'm1'
    """
    m1 = '1min'
    m3 = '3min'
    m5 = '5min'
    m10 = '10min'
    m15 = '15min'
    m30 = '30min'
    h1 = '1hour'
    h2 = '2hour'
    h3 = '3hour'
    h4 = '4hour'
    h6 = '6hour'
    h12 = '12hour'
    d1 = '1day'
    d2 = '2day'
    d3 = '3day'
    d5 = '5day'
    w1 = '1week'
    M1 = '1month'       # 大写的M代表月份
    Y1 = '1year'


class RoutingKeyIncr(Enum):
    """
    说明:
        kline routingkey 标识
    """
    history = -1    # 历史
    not_full = 0    # 实时
    full = 1        # 满柱


class PushType(Enum):
    """
    说明:
        push 类型
    """
    rise_or_fall = 'rise_or_fall'
    pass_price = 'pass_price'
    noon_broadcast = 'noon_broadcast'
