#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-03 20:07

import ujson
import asyncio
import threading

from django.conf import settings

from exspider.utils.redis_con import REDIS_CON as redis_conn

# global
global_thread_loop = None


class ThreadLoop:

    def __init__(self):
        self.loop = None

    def _get_loop(self, loop):
        self.loop = loop
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def init_loop(self):
        new_loop = asyncio.new_event_loop()
        t = threading.Thread(target=self._get_loop, args=(new_loop,))
        t.start()

def get_ret_from_thread_loop(coro):
    """
    功能:
        异步 在一个子线程中执行一个task 任务
    """
    global global_thread_loop
    if not global_thread_loop or global_thread_loop.is_closed():
        thread_loop = ThreadLoop()
        thread_loop.init_loop()
        global_thread_loop = thread_loop.loop
    ret = asyncio.run_coroutine_threadsafe(coro, global_thread_loop)
    return ret.result()


def get_spider_host_map():
    """
    功能:
        获取 spider 服务器的映射, 从缓存读取, 如果异常 方便切换
    return:
        spider_host_map = {
            'spider1': '47.240.24.174',
            'spider2': '149.129.90.193',
            'spider3': '47.240.28.97'
        }
    """
    redis_data = redis_conn.hgetall(settings.SPIDER_HOST_MAP_KEY)
    if redis_data:
        spider_host_map = {
            x.decode(): redis_data[x].decode()
            for x in redis_data
        }
        return spider_host_map
    else:
        return settings.SPIDER_HOST_MAP


def get_alarm_price_coin():
    """
    功能:
        获取 价格异动的币种
    """
    key = settings.PRICE_ALARM_COIN_KEY
    redis_data = redis_conn.get(key)
    if redis_data:
        return ujson.loads(redis_data)
    else:
        redis_conn.set(key, ujson.dumps(settings.PRICE_ALARM_COIN_LIST))
        print('设置覆盖币种: ', settings.PRICE_ALARM_COIN_LIST)
        return settings.PRICE_ALARM_COIN_LIST


def get_pass_price_coin():
    """
    功能:
        获取 整数关口币种缓存
    """
    key = settings.PASS_PRICE_COIN_KEY
    redis_data = redis_conn.get(key)
    if redis_data:
        return ujson.loads(redis_data)
    else:
        redis_conn.set(key, ujson.dumps(settings.PASS_PRICE_COIN_LIST))
        print('设置整数关口币种: ', settings.PASS_PRICE_COIN_LIST)
        return settings.PASS_PRICE_COIN_LIST


def get_alarm_price_push_percent():
    """
    功能:
        获取 价格异动币种触发push 幅度
    """
    key = settings.PAIR_ALARM_PUSH_PERCENT_KEY
    redis_data = redis_conn.hgetall(key)
    if redis_data:
        return {x.decode(): float(redis_data[x]) for x in redis_data}
    else:
        redis_conn.hmset(key, settings.PLUNGE_ALARM_PERCENT_MAP)
        print('设置触发幅度: ', ujson.dumps(settings.PLUNGE_ALARM_PERCENT_MAP))
        return settings.PLUNGE_ALARM_PERCENT_MAP


def get_pair_price_source():
    """
    功能:
        设置 币种交易对源
    """
    key = settings.PAIR_PRICE_SOURCE_CACHE_KEY
    redis_data = redis_conn.hgetall(key)
    if redis_data:
        return {x.decode(): ujson.loads(redis_data[x]) for x in redis_data}
    else:
        raise BaseException('未设置 币种价格预警数据源!!!')