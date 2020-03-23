#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-13 11:07
import re
import copy
import time

import aiomysql
from aiomysql import DatabaseError
from django.conf import settings

GLOBAL_POOL_MAP = {}
import logging

logger = logging.getLogger(__name__)


def convert_table_name(table_name):
    if table_name.startswith('t_'):
        return table_name
    pattern_rule = re.compile(r'[^a-zA-Z0-9$]')
    return 't_' + re.sub(pattern_rule, '_', table_name)


class MySQLBase:

    def __init__(self, database=None, conf=None):
        self.database = database
        self.conf = conf
        self.database_conf = copy.copy(conf)
        self.database_conf['db'] = database
        self.pool = None
        self.max_size = settings.MYSQL_MAX_SIZE

    async def __aenter__(self):
        if self.database in GLOBAL_POOL_MAP and GLOBAL_POOL_MAP[self.database]:
            self.pool = GLOBAL_POOL_MAP[self.database]
        else:
            try:
                self.pool = await aiomysql.create_pool(minsize=1, maxsize=self.max_size, autocommit=True, **self.database_conf)
            except DatabaseError:
                conn = await aiomysql.connect(**self.conf)
                cur = await conn.cursor()
                await cur.execute(f'CREATE DATABASE {self.database};')
                self.pool = await aiomysql.create_pool(minsize=1, maxsize=self.max_size, autocommit=True, **self.database_conf)
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
                self.pool = await aiomysql.create_pool(minsize=1, maxsize=self.max_size, autocommit=True, **self.database_conf)
            except DatabaseError:
                conn = await aiomysql.connect(**self.conf)
                cur = await conn.cursor()
                await cur.execute(f'CREATE DATABASE {self.database};')
                self.pool = await aiomysql.create_pool(minsize=1, maxsize=self.max_size, autocommit=True, **self.database_conf)
            finally:
                GLOBAL_POOL_MAP[self.database] = self.pool
        return self.pool

    async def execute(self, sql):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

    async def executemany(self, sql, data_list):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, data_list)

    async def fetchone(self, sql):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                ret = await cur.fetchone()
                return ret

    async def fetchall(self, sql):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                rows = await cur.fetchall()
                return rows

    async def is_exises_table(self, tbl_name):
        tbl_name = convert_table_name(tbl_name)
        has_tbl_sql = """SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA='{}' AND table_name='{}';""".format(self.database, tbl_name)
        ret = await self.fetchone(has_tbl_sql)
        return True if ret else False

    @property
    async def close(self):
        await self.pool.close()
        return True

    @property
    async def now_tms(self):
        return int(time.time()) // 60 * 60


class TradeMySQL(MySQLBase):

    """
    功能:
        连接trade 数据库
        conf = {
            'user': 'root',
            'password': 'zhuchen',
            'host': '127.0.0.1',
            'port': 33061,
        }
    用法1:
        async with TradeMySQL(database='huobipro', conf=conf) as mysql:
            await mysql.insert_many('btcusdt', [(),(),()...])
    用法2:
        mysql = TradeMySQL(database='huobipro', conf=conf)
        await mysql.init
        await mysql.insert_many('btcusdt', [(),(),()...])
        await mysql.close
    """

    async def create_table(self, table_name):
        """
         功能:
            执行 建表sql
        """
        table_name = convert_table_name(table_name)
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                `tms` int(10) NOT NULL,
                `trade_id` char(128) NOT NULL,
                `direction` char(1) NOT NULL,
                `price` decimal(27,18) NOT NULL,
                `amount` decimal(27,18) NOT NULL,
                KEY `trade_tms` (`tms`) USING BTREE,
                KEY `trade_direction` (`direction`) USING BTREE,
                UNIQUE KEY `trade_id` (`trade_id`) USING BTREE
            ) ENGINE=InnoDB AUTO_INCREMENT=1458 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;
            """
        try:
            await self.execute(create_table_sql)
        except Exception as e:
            logger.error('Create Table Already Exists', e)

    async def is_exises_table(self, tbl_name):
        tbl_name = convert_table_name(tbl_name)
        has_tbl_sql = """SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA='{}' AND table_name='{}';""".format(self.database, tbl_name)
        ret = await self.fetchone(has_tbl_sql)
        if not ret:
            await self.create_table(tbl_name)

    async def insert_many(self, table_name, data_list):
        """
        功能:
            插入 多条k线
        """
        ret = False
        table_name = convert_table_name(table_name)
        await self.is_exises_table(table_name)
        filed_name = "tms, trade_id, direction, price, amount"
        insert_sql = f'''INSERT INTO {table_name} ({filed_name}) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE direction=direction;'''
        try:
            await self.executemany(insert_sql, data_list)
            ret = True
            logger.debug(f'{self.database} {table_name} insert Trades Success!')
        except Exception as e:
            logger.error('Insert Data Error:{}\nSQL: {}'.format(e, table_name))
        finally:
            return ret

    async def select_one_from_table(self, table_name):
        """
        功能:
            查询 1条数据
        """
        table_name = convert_table_name(table_name)
        try:
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, trade_id, direction, price, amount from {table_name} order by tms desc;'''
            return await self.fetchone(select_sql)
        except Exception as e:
            await logger.error(e)
            return

    async def select_table(self, table_name):
        """
        功能:
            查询 多条数据
        """
        table_name = convert_table_name(table_name)
        try:
            await self.is_exises_table(table_name)
            select_sql = f'''SELECT tms, trade_id, direction, price, amount from {table_name} order by tms desc;'''
            rows = await self.fetchall(select_sql)
        except Exception as e:
            await logger.error('select rows error: ', e)
            rows = None
        return rows