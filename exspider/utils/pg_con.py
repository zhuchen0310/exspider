#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-04 15:43
import re
import time

import asyncpg
import copy
from django.conf import settings
from asyncpg.exceptions import InvalidCatalogNameError
from exspider.utils.logger_con import get_logger

logger = get_logger(__name__, is_debug=settings.IS_DEBUG)

GLOBAL_POOL_MAP = {}


def convert_table_name(table_name):
    if table_name.startswith('t_'):
        return table_name
    pattern_rule = re.compile(r'[^a-zA-Z0-9$]')
    return 't_' + re.sub(pattern_rule, '_', table_name)


class PgBase:

    def __init__(self, database=None):
        self.database = database
        self.conf = copy.copy(settings.TRADES_DB_CONF)
        self.conf['database'] = database
        self.pool = None
        self.max_size = settings.PG_MAX_SIZE

    async def __aenter__(self):
        if self.database in GLOBAL_POOL_MAP and GLOBAL_POOL_MAP[self.database]:
            self.pool = GLOBAL_POOL_MAP[self.database]
        else:
            try:
                self.pool = await asyncpg.create_pool(min_size=1, max_size=self.max_size, **self.conf)
            except InvalidCatalogNameError:
                await (await asyncpg.connect(**settings.TRADES_DB_CONF)).execute(f'CREATE DATABASE {self.database};')
                self.pool = await asyncpg.create_pool(min_size=1, max_size=self.max_size, **self.conf)
            finally:
                GLOBAL_POOL_MAP[self.database] = self.pool
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # await self.pool.close()
        ...

    @property
    async def init(self):
        if self.database in GLOBAL_POOL_MAP and GLOBAL_POOL_MAP[self.database]:
            self.pool = GLOBAL_POOL_MAP[self.database]
        else:
            try:
                self.pool = await asyncpg.create_pool(min_size=1, max_size=self.max_size, **self.conf)
            except InvalidCatalogNameError:
                await (await asyncpg.create_pool(**settings.TRADES_DB_CONF)).execute(f'CREATE DATABASE {self.database};')
                self.pool = await asyncpg.create_pool(min_size=1, max_size=self.max_size, **self.conf)
            finally:
                GLOBAL_POOL_MAP[self.database] = self.pool
        return self.pool

    @property
    async def close(self):
        # await self.pool.close()
        return True

    @property
    async def now_tms(self):
        return int(time.time()) // 60 * 60


class TradePg(PgBase):

    """
    功能:
        连接trade 数据库
    用法1:
        async with TradePg('huobipro') as pg:
            await pg.insert_many('btcusdt', [(),(),()...])
    用法2:
        pg = TradePg('huobipro')
        await pg.init
        await pg.insert_many('btcusdt', [(),(),()...])
        await pg.close
    """

    def __init__(self, database=None):
        super().__init__(database)
        self.conf = copy.copy(settings.TRADES_DB_CONF)
        self.conf['database'] = database
        self.pool = None

    async def create_ohlcv_table(self, table_name):
        """
         功能:
            执行 建表sql
        """
        table_name = convert_table_name(table_name)
        create_table_sql = '''CREATE TABLE IF NOT EXISTS "{}" ( ''' + \
                           '''tms int8, ''' + \
                           '''trade_id varchar(128), ''' + \
                           '''direction varchar(2), ''' + \
                           '''price numeric, ''' + \
                           '''amount numeric);'''

        create_trade_id_index_sql = f'CREATE UNIQUE INDEX IF NOT EXISTS {table_name}_trade_id ON {table_name} (trade_id);'
        create_tms_index_sql = f'CREATE INDEX {table_name}_tms ON {table_name} USING btree (tms);'
        try:
            await self.pool.execute(create_table_sql.format(table_name))
            await self.pool.execute(create_tms_index_sql)
            await self.pool.execute(create_trade_id_index_sql)
        except Exception as e:
            await logger.error('Create Table Already Exists', e)

    async def is_exises_table(self, tbl_name):
        tbl_name = convert_table_name(tbl_name)
        has_tbl_sql = """SELECT 1 FROM pg_class WHERE relname = '{}';""".format(tbl_name)
        ret = await self.pool.fetch(has_tbl_sql)
        if not ret:
            await self.create_ohlcv_table(tbl_name)

    async def insert_many(self, table_name, data_list):
        """
        功能:
            插入 多条k线
        """
        table_name = convert_table_name(table_name)
        await self.is_exises_table(table_name)
        filed_name = "tms, trade_id, direction, price, amount"
        insert_sql = f'''INSERT INTO {table_name} ({filed_name}) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (trade_id) do nothing;'''
        try:
            await self.pool.executemany(insert_sql, data_list)
            await logger.debug(f'{self.database} {table_name} insert Trades Success!')
        except Exception as e:
            await logger.error('Insert Data Error:{}\nSQL: {}'.format(e, table_name))

    async def select_one_from_table(self, table_name):
        """
        功能:
            查询 1条数据
        """
        now = await self.now_tms
        try:
            table_name = convert_table_name(table_name)
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, trade_id, direction, price, amount from {table_name} where tms <= {now} order by tms desc;'''
            return await self.pool.fetchrow(select_sql)
        except Exception as e:
            await logger.error(e)
            return

    async def select_table(self, table_name):
        """
        功能:
            查询 多条数据
        """
        now = await self.now_tms
        try:
            table_name = convert_table_name(table_name)
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, trade_id, direction, price, amount from {table_name} where tms <= {now} order by tms desc;'''
            rows = await self.pool.fetch(select_sql)
        except Exception as e:
            await logger.error('select rows error: ', e)
            rows = None
        return rows


class OHLCVPg(PgBase):

    """
    功能:
        连接ohlcv 数据库
    """

    def __init__(self, database=None):
        super().__init__(database)
        self.conf = copy.copy(settings.OHLCV_DB_CONF)
        self.conf['database'] = database
        self.pool = None

    async def create_ohlcv_table(self, table_name):
        """
         功能:
            执行 建表sql
        """
        table_name = convert_table_name(table_name)
        create_table_sql = '''CREATE TABLE IF NOT EXISTS "{}" (''' + \
                           '''tms int8, ''' + \
                           '''open numeric, ''' + \
                           '''high numeric, ''' + \
                           '''low numeric, ''' + \
                           '''close numeric, ''' + \
                           '''volume numeric);'''

        create_index_sql = 'CREATE UNIQUE INDEX IF NOT EXISTS {}_{} ON {} ({});'.format(
            table_name, 'tms', table_name, 'tms')
        try:
            await self.pool.execute(create_table_sql.format(table_name))
            await self.pool.execute(create_index_sql)
        except Exception as e:
            await logger.error('Create Table Already Exists', e)

    async def is_exises_table(self, tbl_name):
        tbl_name = convert_table_name(tbl_name)
        has_tbl_sql = """SELECT 1 FROM pg_class WHERE relname = '{}';""".format(tbl_name)
        ret = await self.pool.fetch(has_tbl_sql)
        if not ret:
            await logger.info("go to create_ohlcv_table: %s" % tbl_name)
            await self.create_ohlcv_table(tbl_name)

    async def insert_many(self, table_name, data_list):
        """
        功能:
            插入 多条k线
        """
        table_name = convert_table_name(table_name)
        await self.is_exises_table(table_name)
        filed_name = "tms, open, high, low, close, volume"
        # insert_sql = f'''INSERT INTO {table_name} ({filed_name}) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tms) do nothing;'''
        insert_sql = f'''INSERT INTO {table_name} ({filed_name}) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tms) '''\
            '''DO UPDATE SET tms=$1, open=$2, high=$3, low=$4, close=$5, volume=$6;'''
        try:
            await self.pool.executemany(insert_sql, data_list)
            # await logger.debug(f'{self.database} {table_name} insert OHLCV Success!')
        except Exception as e:
            await logger.error('Insert Data Error:{}\nSQL: {}'.format(e, table_name))

    async def select_one_from_table(self, table_name):
        """
        功能:
            查询 1条数据
        """
        now = await self.now_tms
        try:
            table_name = convert_table_name(table_name)
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, open, high, low, close, volume from {table_name} where tms <= {now} order by tms desc;'''
            return await self.pool.fetchrow(select_sql)
        except Exception as e:
            await logger.error(e)
            return

    async def select_table(self, table_name):
        """
        功能:
            查询 多条数据
        """
        now = await self.now_tms
        try:
            table_name = convert_table_name(table_name)
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, open, high, low, close, volume from {table_name} where tms <= {now} order by tms desc;'''
            rows = await self.pool.fetch(select_sql)
        except Exception as e:
            await logger.error(f'select {table_name} error: {e}')
            rows = None
        return rows

    async def select_many(self, table_name, from_tms):
        """
        功能:
            查询 多条数据
        """
        now = await self.now_tms
        try:
            table_name = convert_table_name(table_name)
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, open, high, low, close, volume from {table_name} where tms >= {from_tms} and tms <= {now} order by tms asc;'''
            rows = await self.pool.fetch(select_sql)
        except Exception as e:
            await logger.error(f'select {table_name} error: {e}')
            rows = None
        return rows



