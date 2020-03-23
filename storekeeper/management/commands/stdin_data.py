#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-18 14:55

import asyncio
import os
from django.core.management import BaseCommand
from spider import ws_crawl


loop = asyncio.get_event_loop()


class Command(BaseCommand):

    """
    CMD:
        python manage.py stdin_data /opt/base 'all' 'all' kline ENV
                                     csv path  交易所 交易对 类型  配置文件
    """

    def add_arguments(self, parser):
        parser.add_argument('db_path', type=str)
        parser.add_argument('exchange_id_str', type=str)
        parser.add_argument('symbol_str', type=str)
        parser.add_argument('type', type=str)

    def handle(self, *args, **options):
        """
        功能:
            1. 读取csv --> 入库
        """
        db_path = options['db_path']
        exchange_id_str = options['exchange_id_str']
        symbol_str = options['symbol_str']
        type = options['type']
        if not os.path.exists(db_path):
            print(f'Closed... Error db_path: {db_path}')
            return
        if db_path.endswith('/'):
            db_path =os.path.split(db_path)[0]
        if ',' in exchange_id_str:
            exchange_ids = exchange_id_str.split(',')
        elif exchange_id_str == 'all':
            exchange_ids = [x for x in ws_crawl.exchanges if os.path.exists(f'{db_path}/{x}')]
        else:
            exchange_ids = [exchange_id_str]
        if type == 'kline':
            csv_dir = 'kline_csv'
            import_cmd = 'import_ohlcv'
        else:
            csv_dir = 'csv'
            import_cmd = 'import_trade'
        for exchange_id in exchange_ids:
            if ',' in symbol_str:
                symbols = symbol_str.split(',')
            elif symbol_str == 'all':
                symbols = os.listdir(f'{db_path}/{exchange_id}')
            else:
                symbols = [symbol_str]
            for symbol in symbols:
                symbol_path = f'{db_path}/{exchange_id}/{symbol}/{csv_dir}'
                if not os.path.exists(symbol_path):
                    print(f'Continue not Exist: {symbol_path}')
                    continue
                print(f'Start {symbol_path} ...')
                cmd = f'cat {symbol_path}/*.csv | python manage.py {import_cmd} {exchange_id} {symbol} local_dev'
                try:
                    os.system(cmd)
                except Exception as e:
                    print(e)
