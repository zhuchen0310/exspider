# -*- coding:utf-8 -*-
import os
import asyncio
import logging

from django.conf import settings

from exspider.utils.redis_con import StoreKeeperAsyncRedis
from exspider.utils.mysql_con import TradeMySQL
from exspider.utils.logger_con import get_logger

logger = get_logger(__name__)

class ImportLocalTrades:
    def __init__(self, local_dir, loop, exchange_id, symbol, is_forever=False):
        self._local_dir = local_dir
        self._loop = loop
        self._exchange_id = exchange_id
        self._symbol = symbol
        self._trades_pg_dict = {}  # exchange -> pg
        self._store_redis = StoreKeeperAsyncRedis(loop)
        self._exchange_symbol_trade_need_update = {}
        self.is_forever = is_forever    # 持续插入 还是 只是插入一轮
        self.one_time_read_count = 1000 # 每次读取的条数 入库

    async def start(self):
        tasks = []  # task 按照交易所建立, 一个交易所一个task
        # /data1/trades/hk1/binance/btcusdt/csv/1557007828.csv
        local_dir = self._local_dir
        if not os.path.exists(local_dir):
            await logger.error(f'{local_dir} not exists!')
            return
        if not self._exchange_id:
            exchange_ids = [x for x in os.listdir(local_dir) if '.' not in x]
        elif ',' in self._exchange_id:
            exchange_ids = self._exchange_id.split(',')
        else:
            exchange_ids = [self._exchange_id]

        if not self._symbol:
            symbols = []
        elif ',' in self._symbol:
            symbols = self._symbol.split(',')
        else:
            symbols = [self._symbol]

        for exchange_id in exchange_ids:
            exchange_path = f'{local_dir}/{exchange_id}'
            if not os.path.exists(exchange_path):
                continue
            tasks.append(self._loop.create_task(self.import_task(exchange_path, exchange_id, symbols)))
        await asyncio.gather(*tasks)

    async def import_machine_dir(self, exchange_path, exchange_id, symbols):
        """
        功能:
            交易所 Task, 轮训批量 插入trades
        """
        spider_name = self._local_dir.split('/')[-1]
        await logger.info(f'Start Task: {exchange_id}')
        await asyncio.sleep(1)
        if not symbols:
            symbols = [x for x in os.listdir(exchange_path) if '.' not in x]
        for symbol in symbols:
            exchange_dir = exchange_id
            symbol_dir = symbol
            machine_path = exchange_path + '/' + symbol
            trade_csv_dir_path = machine_path + '/csv'
            if os.path.exists(trade_csv_dir_path) and os.path.isdir(trade_csv_dir_path):
                await logger.info("start to check directory: %s" % trade_csv_dir_path)
                exchange_symbol = f"""{exchange_dir}_{symbol_dir}"""
                # 每次 只读 上次那个和本次最新的一个文件, 没必要全部读
                last_file_time = await self._store_redis.get_import_data_last_mtime(
                    f'trade_{exchange_dir}', symbol_dir, spider_name=spider_name)
                if last_file_time <= 1563003960:
                    last_file_time = 0
                try:
                    new_file_time = max(
                        [int(file.replace('.csv', '')) for file in os.listdir(trade_csv_dir_path) if 'csv' in file])
                except Exception as e:
                    await logger.error(f'not has kline_csv: {trade_csv_dir_path} {e}')
                    return
                if last_file_time and last_file_time != new_file_time:
                    # 读 最新的两个
                    csv_file_list = [f'{last_file_time}.csv', f'{new_file_time}.csv']
                    # 设置新的读取文件
                    await self._store_redis.set_import_data_last_mtime(f'trade_{exchange_dir}', symbol_dir,
                                                                       new_file_time,
                                                                       spider_name=spider_name)
                else:
                    csv_file_list = [f'{new_file_time}.csv']
                for csv_file in csv_file_list:
                    csv_file_path = trade_csv_dir_path + '/' + csv_file
                    await logger.info(f"check {spider_name}, {exchange_symbol}: {csv_file}")
                    await self.import_trade_file(exchange_dir, symbol_dir, csv_file_path)

    async def import_task(self, exchange_path, exchange_id, symbols):
        if self.is_forever:
            while True:
                await self.import_machine_dir(exchange_path, exchange_id, symbols)
                await asyncio.sleep(60 * 60)
        else:
            await self.import_machine_dir(exchange_path, exchange_id, symbols)

    async def import_trade_file(self, exchange_id, symbol, file_path_name):
        # logger.debug("import trade file: %s:%s, %s" % (exchange_id, symbol, file_path_name))
        # 设置 exchange_symbol 的历史数据需要更新
        spider_name = self._local_dir.split('/')[-1]
        exchange_symbol = f"""{exchange_id}_{symbol}"""
        if exchange_symbol in self._exchange_symbol_trade_need_update:
            (from_ts, to_ts) = self._exchange_symbol_trade_need_update[exchange_symbol]
        else:
            from_ts = 29958289229
            to_ts = 0
        pg = await self.get_trades_pg(exchange_id)
        table_name = f'{symbol}'
        data_list = []
        file_name = os.path.split(file_path_name)[-1]
        last_line_no = await self._store_redis.get_pair_last_read_no(f'trade_{exchange_id}', symbol, file_name, spider_name=spider_name)
        await logger.info(f'{spider_name}: {exchange_symbol} -> start trade from lineno: {last_line_no}')
        with open(file_path_name) as fh:
            line_no = 0
            for line_no, line in enumerate(fh):
                if line_no <= last_line_no:
                    continue
                try:
                    trade = await self.handle_csv_line(line)
                    ts = trade[0]
                    if ts > 4082703248 or ts < 11794448:
                        await logger.error("Find a error timestamp while parsing line: %s of file: %s." % (
                            line, file_path_name))
                        continue
                    if ts < from_ts:
                        from_ts = ts
                    if ts > to_ts:
                        to_ts = ts
                    data_list.append(trade)
                    if len(data_list) >= self.one_time_read_count:
                        while True:
                            if await pg.insert_many(table_name, data_list):
                                break
                            await asyncio.sleep(0.001)
                        data_list = []
                        await self._store_redis.set_pair_last_read_no(f'trade_{exchange_id}', symbol, file_name, line_no, spider_name=spider_name)
                except Exception as e:
                    await logger.error("Exception to parse line: %s of file: %s. %s" % (line, file_path_name, e))
        if len(data_list) > 0:
            while True:
                if await pg.insert_many(table_name, data_list):
                    break
                await asyncio.sleep(0.001)
            await self._store_redis.set_pair_last_read_no(f'trade_{exchange_id}', symbol, file_name, line_no,
                                                          spider_name=spider_name)
        self._exchange_symbol_trade_need_update[exchange_symbol] = (from_ts, to_ts)

    async def get_trades_pg(self, exchange_id):
        conf = settings.TIDB_CONF
        if exchange_id not in self._trades_pg_dict:
            pg = TradeMySQL(exchange_id, conf=conf)
            await pg.init
            self._trades_pg_dict[exchange_id] = pg
        else:
            pg = self._trades_pg_dict[exchange_id]
        return pg

    async def handle_csv_line(self, line):
        """
        功能:
            处理成 入库格式
            [
                timestamp int,
                trade_id str,
                direction str,
                price float,
                amount float
            ]
        """
        line = line.strip()
        if not line:
            return
        if 'price' in line:
            return
        if 'sell' in line:
            trade_str = line.replace('sell', 's')
        elif 'ask' in line:
            trade_str = line.replace('ask', 's')
        elif 'bid' in line:
            trade_str = line.replace('bid', 'b')
        elif 'buy' in line:
            trade_str = line.replace('buy', 'b')
        else:
            trade_str = line
        trade = trade_str.split(',')
        return [
            int(trade[0]),
            trade[1],
            trade[2],
            float(trade[3]),
            float(trade[4]),
        ]





