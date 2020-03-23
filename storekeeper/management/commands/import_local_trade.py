# -*- coding:utf-8 -*-
import asyncio
from django.core.management import BaseCommand
from storekeeper.import_local_trades import ImportLocalTrades


class Command(BaseCommand):
    """
    CMD:
        python manage.py import_local_trade  /data1/trades/hk1 huobipro btcusdt true storekeeper_dev
        本地目录结构：/data1/trades/hk1/binance/btcusdt/csv/1557007828.csv
    """
    def add_arguments(self, parser):
        parser.add_argument('local_dir', type=str)
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)
        parser.add_argument('is_forever', type=str)

    def handle(self, *args, **options):
        local_dir = options['local_dir']
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        is_forever = True if options['is_forever'] in ['1', 'true', 'True'] else False
        loop = asyncio.get_event_loop()
        import_local_data = ImportLocalTrades(local_dir, loop, exchange_id, symbol, is_forever=is_forever)
        loop.run_until_complete(import_local_data.start())


