#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-03 16:47
import ujson
import time
import datetime
import asyncio

from django.core.management import BaseCommand
from django.conf import settings
from exspider.utils.redis_con import REDIS_CON
from exspider.utils.enum_con import SpiderStatus
from spider.ws_crawl import WS_TYPE_TRADE, WS_TYPE_KLINE
from spider import ws_crawl
from exspider.utils import redis_con as redis


AUTO_STOP_TMS = settings.SYMBOL_AUTO_STOP_TMS
check_new_symbol_tms = settings.CHECK_NEW_SYMBOL_TMS
# 交易所状态
S_EXCHANGE_STATUS_KEY_MAP = settings.EXCHANGE_STATUS_KEY_MAP
# 交易所spider
S_SPIDER_STATUS_KEY_MAP = settings.SPIDER_STATUS_KEY_MAP
# 交易所交易对
S_SYMBOL_STATUS_KEY_MAP = settings.SYMBOL_STATUS_KEY_MAP

STATUS_KEY = 's'
TMS_KEY = 't'
PID_KEY = 'p'

loop = asyncio.get_event_loop()
proxy = settings.PROXY

class Command(BaseCommand):
    """
    CMD:
        python manage.py auto_set_spider_status
    功能:
        定时监控 spider 状态 对于长时间不更新的进行 状态重置
        定时 删除 和新增 symbol
        定时 如无空闲spider 启动 新的spider发送订阅
    """
    def handle(self, *args, **options):
        check_symbol_start_tms = 0
        check_status_start_tms = 0
        try:
            while 1:
                print(datetime.datetime.now(), 'Start!')
                if not check_status_start_tms or int(time.time()) - check_status_start_tms > AUTO_STOP_TMS:
                    for ws_type in [WS_TYPE_TRADE, WS_TYPE_KLINE]:
                        self.handle_ws_type_exchanges(ws_type)
                    else:
                        check_status_start_tms = int(time.time())

                if not check_symbol_start_tms or int(time.time()) - check_symbol_start_tms > check_new_symbol_tms:
                    for ws_type in [WS_TYPE_TRADE, WS_TYPE_KLINE]:
                        for exchange_id in ws_crawl.exchanges:
                            self.fix_exchange_symbols(exchange_id, ws_type)
                    else:
                        print('完成一次新交易对检查')
                        check_symbol_start_tms = int(time.time())
                time.sleep(min([AUTO_STOP_TMS, check_new_symbol_tms]))
        except KeyboardInterrupt:
            print('退出!')
            exit(0)

    def handle_ws_type_exchanges(self, ws_type):
        if not ws_type:
            print(f'{ws_type} Error !!!')
            return
        all_exchange_ids = (x.decode() for x in REDIS_CON.hkeys(S_EXCHANGE_STATUS_KEY_MAP[ws_type]))
        for exchange_id in all_exchange_ids:
            spider_key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
            spider_data = REDIS_CON.hgetall(spider_key)
            for pid in spider_data:
                now = int(time.time())
                pid_data = ujson.loads(spider_data[pid])
                if now - pid_data[TMS_KEY] > AUTO_STOP_TMS:
                    print(f'{datetime.datetime.now()} 停止 {exchange_id} {pid}')
                    pid_data[STATUS_KEY] = SpiderStatus.stopped.value
                    pid_data[TMS_KEY] = now
                    REDIS_CON.hset(spider_key, pid, ujson.dumps(pid_data))
        print(f'{datetime.datetime.now()} 完成一次 {ws_type} 监控!')

    def fix_exchange_symbols(self, exchange_id, ws_type):
        """
        功能:
            同步交易所symbol
        """
        ex_symbols_key = settings.SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
        symbols_data = REDIS_CON.hgetall(ex_symbols_key)
        symbols_map = {x.decode(): ujson.loads(symbols_data[x]) for x in symbols_data}
        ex = getattr(ws_crawl, exchange_id)(loop=loop, http_proxy=proxy, ws_proxy=proxy)
        delete_symbols = [x for x in symbols_map if x not in ex.symbols]
        for s in delete_symbols:
            print(f'{exchange_id} {ws_type} 删除{s}')
            REDIS_CON.hdel(ex_symbols_key, s)
        # 添加 交易所新增的交易对
        add_symbols = [x for x in ex.symbols if x not in symbols_map]
        add_symbols_map = {
            x: ujson.dumps({
                redis.TMS_KEY: int(time.time()),
                redis.STATUS_KEY: SpiderStatus.pending.value,
                redis.PID_KEY: ''})
            for x in add_symbols
        }
        if add_symbols_map:
            print(f'{exchange_id} {ws_type} 新增交易对: {add_symbols}')
            REDIS_CON.hmset(ex_symbols_key, add_symbols_map)

