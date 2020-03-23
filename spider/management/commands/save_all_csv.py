#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-03 16:47
from django.core.management import BaseCommand
from spider.tasks import parse_exchange_trade_csv
from django.conf import settings


class Command(BaseCommand):
    """
    CMD:
        python manage.py save_all_csv huobipro btcusdt kline ''
    功能:
        将指定交易所 交易对 Trade 保存到 csv 中, 同时启动一个 forever 的 loop 监听 需要生成的 symbol:exchange
        is_forever @: 是否启动无限循环
    """

    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)
        parser.add_argument('ws_type', type=str)
        parser.add_argument('is_forever', type=bool)

    def handle(self, *args, **options):
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        ws_type = options['ws_type']
        is_forever = options['is_forever']
        if ws_type not in settings.EXCHANGE_STATUS_KEY_MAP:
            print(f'{ws_type} is Error!')
            return
        parse_exchange_trade_csv(exchange_id, symbol, is_forever=is_forever, ws_type=ws_type)