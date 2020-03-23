# -*- coding:utf-8 -*-
import ujson
import asyncio
from django.core.management import BaseCommand
from storekeeper.import_local_data import ImportLocalData
from exspider.utils.enum_con import TimeFrame


class Command(BaseCommand):
    """
    CMD:
        python manage.py import_local_data  /data1/trades/hk1  storekeeper_dev
        python manage.py import_local_data  /data1/trades/hk1 --exchange_id=binance --symbol=btcusdt storekeeper_dev
        本地目录结构：/data1/trades/hk1/binance/btcusdt/kline_csv/1557007828.csv
    """
    def add_arguments(self, parser):
        parser.add_argument('local_dir', type=str)
        # Named (optional) arguments
        parser.add_argument('--exchange_id', dest='exchange_id', default='')
        parser.add_argument('--symbol', dest='symbol', default='')

    def handle(self, *args, **options):
        local_dir = options['local_dir']
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        loop = asyncio.get_event_loop()
        import_local_data = ImportLocalData(local_dir, loop, exchange_id, symbol)
        loop.run_until_complete(import_local_data.run())


