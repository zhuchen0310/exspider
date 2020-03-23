#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-23 15:09

import time
import ujson
import datetime
from django.conf import settings
from django.core.management import BaseCommand

from exspider.utils.redis_con import REDIS_CON
from exspider.utils.enum_con import TimeFrame
from exspider.utils.socket_server import send_notice_to_php
from storekeeper.alarm.base import get_exchange_pair_data
from exspider.common import funcs

S_CHANGE_PAIR_PRICE_TMS = settings.CHANGE_PAIR_PRICE_TMS

S_PAIR_PRICE_SOURCE_CACHE = funcs.get_pair_price_source()


class Command(BaseCommand):
    help = 'python manage.py care_pair_push'

    def handle(self, *args, **options):
        """
        功能:
            将行情异动监测交易对由 主交易对 依次切换为替补交易对1 例：币安 BTC/USDT 切换成 火币 BTC/USDT
            替补交易对1 依旧不更新时将继续切换成 替补交易对2  例：火币 BTC/USDT 切换成 OKEx BTC/USDT
            主交易对继续更新后，将火币 替补交易对 自动切回 主交易对
            替补交易对将由该交易对加权成交额由高到低依次选取
            全网全部交易对均无更新则不进行推送

        :param args:
        :param options:
        :return:
        """
        try:
            while 1:
                self.main()
                time.sleep(S_CHANGE_PAIR_PRICE_TMS)
        except KeyboardInterrupt:
            exit(0)

    def main(self):
        for base_name in S_PAIR_PRICE_SOURCE_CACHE:
            print(f'Start {base_name} {datetime.datetime.now()}...')
            sorted_pair_list = [x[0] for x in sorted(S_PAIR_PRICE_SOURCE_CACHE[base_name], key=lambda x: x[1], reverse=True)]
            base_name = base_name.lower()
            first_symbol_exchange = sorted_pair_list[0]
            key = settings.COIN_PUSH_FROM_PAIR_KEY
            symbol_exchange_data = REDIS_CON.hget(key, base_name)
            if symbol_exchange_data:
                symbol_exchange = symbol_exchange_data.decode()
                now_source = symbol_exchange
                if not self.check_kline_is_update(symbol_exchange):
                    print(f'{datetime.datetime.now()} {base_name} now_source: {symbol_exchange} disconnect!')
                    for new_symbol_exchange in sorted_pair_list:
                        if self.check_kline_is_update(new_symbol_exchange):
                            print(f'{datetime.datetime.now()} change_source -> {new_symbol_exchange}')
                            change_source = new_symbol_exchange
                            REDIS_CON.hset(key, base_name, new_symbol_exchange)
                            break
                    else:
                        print(f'{datetime.datetime.now()} {base_name} all source is disconnect, change -> base source {first_symbol_exchange}!!!')
                        REDIS_CON.hset(key, base_name, first_symbol_exchange)
                        change_source = first_symbol_exchange
                else:
                    print(f'{datetime.datetime.now()} {base_name} now_source: {symbol_exchange} OK!')
                    change_source = symbol_exchange
                    if symbol_exchange != first_symbol_exchange:
                        if self.check_kline_is_update(first_symbol_exchange):
                            print(f'{datetime.datetime.now()} {base_name} change_to_base: {symbol_exchange} >> {first_symbol_exchange}')
                            change_source = first_symbol_exchange
                            REDIS_CON.hset(key, base_name, first_symbol_exchange)
            else:
                print(f'{datetime.datetime.now()} {base_name} cache {base_name.upper()} has no source!!!')
                print(f'{datetime.datetime.now()} {base_name} init source: {first_symbol_exchange}')
                REDIS_CON.hset(key, base_name, first_symbol_exchange)
                now_source = change_source = first_symbol_exchange
            if base_name in ['btc', 'eth']:
                # btc eth 分析宝 通知PHP
                if now_source != change_source:
                    symbol, exchange_id = change_source.split(':')
                    pair_data = funcs.get_ret_from_thread_loop(get_exchange_pair_data(exchange_id, base_name, quote_name='usdt'))
                    pair_id = pair_data['pair_id']
                    pair_id_list = [
                        funcs.get_ret_from_thread_loop(get_exchange_pair_data(x.split(':')[1], base_name, quote_name='usdt'))['pair_id']
                        for x in [source[0] for source in S_PAIR_PRICE_SOURCE_CACHE[base_name.upper()]]]
                    data = {
                        'pair_id': pair_id,
                        'pair_id_list': pair_id_list
                    }
                    send_notice_to_php(settings.PHP_NOTICE_METHOD['fenxibao'], data=data)
                    print(now_source, change_source, data)

    def check_kline_is_update(self, symbol_exchange):
        """
        功能:
            通过缓存 查询数据是否在更新
        """
        symbol, exchange_id = symbol_exchange.split(':')
        try:
            last_ohlcv = REDIS_CON.hget(settings.EXCHANGE_LAST_OHLCV_KEY.format(exchange_id), f'{symbol}_{TimeFrame("1min").name}')
            if last_ohlcv:
                data = ujson.loads(last_ohlcv)
                last_tms = data[0]
                if int(time.time()) - last_tms >= S_CHANGE_PAIR_PRICE_TMS:
                    return False
            else:
                return False
        except:
            ...
        return True