#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-14 18:23
import csv
import asyncio
import time

from django.core.management import BaseCommand
from django.conf import settings

from exspider.utils.mysql_con import MySQLBase


class Command(BaseCommand):

    EXCHANGE_SQL_POOL = {}
    start_tms = 1558972800

    def handle(self, *args, **options):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.go())

    async def go(self):
        def handle(data, exchange_id, symbol):
            ret = [exchange_id, symbol, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data[0])), float(data[1])]
            return ret
        exchanges = ['binance', 'huobipro', 'okex']

        symbols = [
            'btcusdt',
            'ethusdt',
            'xrpusdt',
            'bchusdt',
            'ltcusdt',
            'eosusdt',
            'bnbusdt',
            'htusdt',
            'okbusdt',
            'etcusdt'
        ]

        for exchange_id in exchanges:
            db = MySQLBase(exchange_id, conf=settings.TIDB_CONF)
            await db.init
            self.EXCHANGE_SQL_POOL[exchange_id] = db

        for symbol in symbols:
            rows = []
            for exchange_id in exchanges:
                print(exchange_id, symbol)
                db = self.EXCHANGE_SQL_POOL[exchange_id]
                if not await db.is_exises_table(symbol):
                    continue
                data = await db.fetchall(self.get_select_sql(symbol))
                if data:
                    rows.extend([handle(x, exchange_id, symbol) for x in data])
            if not rows:
                continue
            rows = sorted(rows, key=lambda x: x[3], reverse=True)
            with open(f'{symbol}.csv', 'a+', newline='') as f:
                f_csv = csv.writer(f)
                f_csv.writerows(rows)


    def get_select_sql(self, symbol):

        sql = f"""select tms, amount from t_{symbol} where tms >= {self.start_tms} order by amount desc limit 100;"""
        return sql