#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:59

from django.core.management import BaseCommand

from spider.tasks import start_spider, stop_spider
from django.conf import settings
from exspider.utils.logger_con import get_logger
logger = get_logger(__name__, is_debug=settings.IS_DEBUG)

class Command(BaseCommand):

    """
    CMD:
        python manage.py set_spider huobipro btcusdt kline start dev
    说明:
        一次 设置多个交易所不能指定交易对, 所有交易对都会被设置
        多个交易所, 交易对 用, 隔开
    """
    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)
        parser.add_argument('ws_type', type=str)
        parser.add_argument('spider_type', type=str)


    def handle(self, *args, **options):
        spider_map = {
            'start': start_spider,
            'stop': stop_spider
        }
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        ws_type = options['ws_type']
        if ws_type not in settings.EXCHANGE_STATUS_KEY_MAP:
            print(f'{ws_type} is Error!')
            return
        spider_type = options['spider_type']
        spider_func = spider_map[spider_type]

        if not exchange_id or exchange_id in ['all', '']:
            exchange_ids = ['']
            symbols = ''
        elif ',' in exchange_id:
            exchange_ids = exchange_id.split(',')
            symbols = ''
        else:
            exchange_ids = [exchange_id]
            if not symbol or symbol in ['all', '']:
                symbols = ''
            elif ',' in symbol:
                symbols = symbol.split(',')
            else:
                symbols = [symbol]

        for ex_id in exchange_ids:
            if symbols:
                for _symbol in symbols:
                    spider_func(ex_id, _symbol, ws_type)
            else:
                spider_func(ex_id, symbols, ws_type)
