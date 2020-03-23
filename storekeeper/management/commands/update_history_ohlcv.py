# -*- coding:utf-8 -*-
import ujson
import asyncio
from django.core.management import BaseCommand
from storekeeper.import_local_data import ImportLocalData
from exspider.utils.enum_con import TimeFrame


class Command(BaseCommand):
    """
    CMD:
        python manage.py update_history_ohlcv binance btcusdt 15min 1557192600 1557193500 storekeeper_dev
    """
    def add_arguments(self, parser):
        parser.add_argument('exchange_id', type=str)
        parser.add_argument('symbol', type=str)
        parser.add_argument('time_frame', type=str)
        parser.add_argument('from_ts', type=float)
        parser.add_argument('to_ts', type=float)

    def handle(self, *args, **options):
        exchange_id = options['exchange_id']
        symbol = options['symbol']
        time_frame = options['time_frame']
        from_ts = options['from_ts']
        to_ts = options['to_ts']
        loop = asyncio.get_event_loop()
        import_local_data = ImportLocalData('', loop, exchange_id, symbol)
        loop.run_until_complete(import_local_data.update_history_ohlcv(exchange_id, symbol, time_frame, from_ts, to_ts))
        loop.close()


