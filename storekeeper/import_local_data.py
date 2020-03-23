# -*- coding:utf-8 -*-
import os
import ujson
import time
import datetime
import copy
import asyncio
from exspider.utils.logger_con import get_logger
from exspider.utils.amqp_con import AioPikaConsumer
from exspider.utils.redis_con import StoreKeeperAsyncRedis
from exspider.utils.pg_con import OHLCVPg
from exspider.utils.pg_con import TradePg
from exspider.utils.enum_con import TimeFrame
from exspider.utils.calc_kline import PERIOD_INTERVAL_MAP, calc_kline
from django.conf import settings
import tracemalloc

logger = get_logger(__name__, is_debug=settings.IS_DEBUG)


REALTIME_CALC_TIMEFRAME_LIST = [
    TimeFrame.m15.value, TimeFrame.h1.value, TimeFrame.d1.value
]

LAST_MTIME_MAX_FILE = 'import_local_data_last_mtime.json'


class ImportLocalData(object):
    def __init__(self, local_dir, loop, exchange_id, symbol):
        self._local_dir = local_dir
        self._loop = loop
        self._exchange_id = exchange_id
        self._symbol = symbol
        # self._last_mtime_max = {}  # exchange_symbol -> mtime
        # self._this_mtime_max = {}  # exchange_symbol -> mtime
        self._exchange_symbol_history_ohlcv_need_update = {}  # exchange_symbol -> (from_ts, to_ts)
        self._ohlcv_pg_dict = {}  # exchange -> pg
        self._trades_pg_dict = {}  # exchange -> pg
        self._store_redis = StoreKeeperAsyncRedis(loop)

    async def reset(self):
        # self._last_mtime_max = {}  # exchange_symbol -> mtime
        # self._this_mtime_max = {}  # exchange_symbol -> mtime
        self._exchange_symbol_history_ohlcv_need_update = {}  # exchange_symbol -> (from_ts, to_ts)
        for key, pg in self._ohlcv_pg_dict.items():
            await pg.close
        self._ohlcv_pg_dict = {}  # exchange -> pg
        for key, pg in self._trades_pg_dict.items():
            await pg.close
        self._trades_pg_dict = {}  # exchange -> pg

    async def import_kline_file(self, exchange_id, symbol, file_path_name):
        await logger.debug("import kline file: %s:%s, %s" % (exchange_id, symbol, file_path_name))
        # 设置 exchange_symbol 的历史数据需要更新
        exchange_symbol = f"""{exchange_id}_{symbol}"""
        if exchange_symbol in self._exchange_symbol_history_ohlcv_need_update:
            (from_ts, to_ts) = self._exchange_symbol_history_ohlcv_need_update[exchange_symbol]
        else:
            from_ts = 29958289229
            to_ts = 0
        pg = await self.get_ohlcv_pg(exchange_id)
        table_name = f'{symbol}_1min'
        data_list = []
        spider_name = file_path_name.replace(f'{self._local_dir}/', '').split('/')[0]
        file_name = os.path.split(file_path_name)[-1]
        last_line_no = await self._store_redis.get_pair_last_read_no(exchange_id, symbol, file_name, spider_name=spider_name)
        await logger.info(f'{spider_name}: {exchange_symbol} -> start kline from lineno: {last_line_no}')
        with open(file_path_name) as fh:
            line_no = 0
            for line_no, line in enumerate(fh):
                if line_no <= last_line_no:
                    continue
                try:
                    cols = line.rstrip().split(",")
                    # await logger.debug("cols:%s" % cols)  # 1556813340,5534.5,5536.37,5529.19,5529.9,21.206805
                    ts = float(cols[0])
                    if ts > 4082703248 or ts < 11794448:
                        await logger.error("Find a error timestamp while parsing line: %s of file: %s." % (
                            line, file_path_name))
                        continue
                    if ts < from_ts:
                        from_ts = ts
                    if ts > to_ts:
                        to_ts = ts
                    data_list.append([ts, float(cols[1]), float(cols[2]), float(cols[3]), float(cols[4]), float(cols[5])])
                    if len(data_list) >= 100:
                        await pg.insert_many(table_name, data_list)
                        data_list = []
                        await self._store_redis.set_pair_last_read_no(exchange_id, symbol, file_name, line_no, spider_name=spider_name)
                except Exception as e:
                    await logger.error("Exception to parse line: %s of file: %s." % (line, file_path_name))
        if len(data_list) > 0:
            await pg.insert_many(table_name, data_list)
            await self._store_redis.set_pair_last_read_no(exchange_id, symbol, file_name, line_no, spider_name=spider_name)
        self._exchange_symbol_history_ohlcv_need_update[exchange_symbol] = (from_ts, to_ts)

    async def update_history_ohlcv(self, exchange_id, symbol, time_frame, from_ts, to_ts):
        align_from_ts = int(from_ts / (PERIOD_INTERVAL_MAP[time_frame] * 60)) * (PERIOD_INTERVAL_MAP[time_frame] * 60)
        align_to_ts = (int(to_ts / (PERIOD_INTERVAL_MAP[time_frame] * 60)) + 1) * (PERIOD_INTERVAL_MAP[time_frame] * 60)
        await logger.debug("update_history_ohlcv: %s:%s, %s, %s ~ %s." % (exchange_id, symbol, time_frame, align_from_ts, align_to_ts))
        pg = await self.get_ohlcv_pg(exchange_id)
        table_name_1min = f'{symbol}_1min'
        table_name_new = f'{symbol}_{time_frame}'
        rows = await pg.select_many(table_name_1min, align_from_ts)
        ret_bar_list = calc_kline(rows, time_frame, skip_header=False, skip_tail=True)
        if len(ret_bar_list) > 0:
            await pg.insert_many(table_name_new, ret_bar_list)
            await logger.debug("insert_many %s bars: %s:%s, %s ~ %s." % (
                len(ret_bar_list), exchange_id, table_name_new, ret_bar_list[0][0], ret_bar_list[-1][0]))
            # await logger.debug("insert_many first bars: %s:%s, %s." % (exchange_id, table_name_new, ret_bar_list[0]))

    async def run(self):
        while True:
            try:
                await self.start()
                await self.reset()
                await logger.info("Finished once check.")
            except Exception as e:
                await logger.error("Exception to run. %s." % e)
            await asyncio.sleep(5 * 60)

    async def start(self):
        # /data1/trades/hk1/binance/btcusdt/kline_csv/1557007828.csv
        local_dir = self._local_dir
        machine_dirs = os.listdir(local_dir)
        for machine_dir in machine_dirs:
            machine_path = local_dir + '/' + machine_dir
            if os.path.isdir(machine_path):
                if machine_dir[0] == '.':
                    pass
                else:
                    await self.import_machine_dir(machine_path)

    async def import_machine_dir(self, machine_path):
        await logger.info("start to check directory: %s" % machine_path)
        exchange_dirs = os.listdir(machine_path)
        spider_name = machine_path.replace(f'{self._local_dir}/', '')
        for exchange_dir in exchange_dirs:
            if self._exchange_id != '' and exchange_dir != self._exchange_id:
                continue
            exchange_path = machine_path + '/' + exchange_dir
            # await logger.debug("exchange path: %s" % exchange_path)
            if os.path.isdir(exchange_path):
                if exchange_dir[0] == '.':
                    pass
                else:
                    symbol_dirs = os.listdir(exchange_path)
                    for symbol_dir in symbol_dirs:
                        if self._symbol != '' and symbol_dir != self._symbol:
                            continue
                        symbol_path = exchange_path + '/' + symbol_dir
                        # await logger.debug("symbol path: %s" % symbol_path)
                        if os.path.isdir(symbol_path):
                            if symbol_dir[0] == '.':
                                pass
                            else:
                                kline_csv_dir_path = symbol_path + '/kline_csv'
                                if os.path.exists(kline_csv_dir_path) and os.path.isdir(kline_csv_dir_path):
                                    exchange_symbol = f"""{exchange_dir}_{symbol_dir}"""
                                    # 每次 只读 上次那个和本次最新的一个文件, 没必要全部读
                                    last_file_time = await self._store_redis.get_import_data_last_mtime(
                                        exchange_dir, symbol_dir, spider_name=spider_name)
                                    if last_file_time <= 1563003960:
                                        last_file_time = 0
                                    try:
                                        new_file_time = max([int(file.replace('.csv', '')) for file in os.listdir(kline_csv_dir_path) if 'csv' in file])
                                    except Exception as e:
                                        await logger.error(f'not has kline_csv: {kline_csv_dir_path}')
                                        return
                                    if last_file_time and last_file_time != new_file_time:
                                        csv_file_list = [f'{last_file_time}.csv', f'{new_file_time}.csv']
                                        # 设置新的读取文件
                                        await self._store_redis.set_import_data_last_mtime(exchange_dir, symbol_dir,
                                                                                           new_file_time,
                                                                                           spider_name=spider_name)
                                    else:
                                        csv_file_list = [f'{new_file_time}.csv']
                                    for csv_file in csv_file_list:
                                        csv_file_path = kline_csv_dir_path + '/' + csv_file
                                        await logger.info(f"check {spider_name}, {exchange_symbol}: {csv_file}")
                                        await self.import_kline_file(exchange_dir, symbol_dir, csv_file_path)
                                        if exchange_symbol in self._exchange_symbol_history_ohlcv_need_update:
                                            from_to_ts = self._exchange_symbol_history_ohlcv_need_update[exchange_symbol]
                                            for time_frame in REALTIME_CALC_TIMEFRAME_LIST:
                                                await self.update_history_ohlcv(exchange_dir, symbol_dir, time_frame,
                                                                                from_to_ts[0], from_to_ts[1])


    async def get_ohlcv_pg(self, exchange_id, time_frame=''):
        # pg_key = f'{exchange_id}_{time_frame}'
        pg_key = f'{exchange_id}'
        if pg_key not in self._ohlcv_pg_dict:
            pg = OHLCVPg(exchange_id)
            await pg.init
            self._ohlcv_pg_dict[pg_key] = pg
        else:
            pg = self._ohlcv_pg_dict[pg_key]
        return pg

    async def get_trades_pg(self, exchange_id):
        if exchange_id not in self._trades_pg_dict:
            pg = TradePg(exchange_id)
            await pg.init
            self._trades_pg_dict[exchange_id] = pg
        else:
            pg = self._trades_pg_dict[exchange_id]
        return pg

    async def insert_ohlcv(self, time_frame, data_json):
        pg = await self.get_ohlcv_pg(data_json['e'], time_frame)
        await pg.insert_many(f'{data_json["s"]}_{time_frame}', [data_json['d']])

    async def insert_trades(self, data_json):
        pg = await self.get_trades_pg(data_json['e'])
        await pg.insert_many(data_json['s'], data_json['d'])

    async def query_ohlcv(self, exchange, symbol, time_frame, from_tms):
        pg = await self.get_ohlcv_pg(exchange, time_frame)
        rows = await pg.select_many(f'{symbol}_{time_frame}', from_tms)
        return rows




