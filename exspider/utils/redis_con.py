#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-06 14:10

import asyncio
import aioredis
import ujson
import time
import redis
from django.conf import settings
from exspider.utils.logger_con import get_logger
from exspider.utils.enum_con import SpiderStatus, TimeFrame


_ASYNC_REDIS_MAP = {}  # global redis_pool_map

# 交易所状态
S_EXCHANGE_STATUS_KEY_MAP = settings.EXCHANGE_STATUS_KEY_MAP
# 交易所spider
S_SPIDER_STATUS_KEY_MAP = settings.SPIDER_STATUS_KEY_MAP
# 交易所交易对
S_SYMBOL_STATUS_KEY_MAP = settings.SYMBOL_STATUS_KEY_MAP

# csv 消息缓存的map
S_EXCHANGE_SYMBOL_CACHE_KEY_MAP = settings.EXCHANGE_SYMBOL_CACHE_KEY_MAP

# # 交易所交易对 Trades_list
# S_EXCHANGE_SYMBOL_TRADE_KEY = settings.EXCHANGE_SYMBOL_TRADE_KEY
# 有用户订阅用于计算ohlcv 的trade_list
S_EXCHANGE_SYMBOL_OHLCV_TRADE_KEY = settings.EXCHANGE_SYMBOL_OHLCV_TRADE_KEY
# 交易所交易对 ohlcv
S_EXCHANGE_LAST_OHLCV_KEY = settings.EXCHANGE_LAST_OHLCV_KEY
# 待生成csv指令
S_PENDING_CSV_MAP = settings.PENDING_CSV_EXCHANGE_KEY_MAP
# redis 异步锁
exchange_task_lock_key = settings.EXCHANGE_TASK_LOCK_KEY
# 用户订阅交易对
S_USER_SUB_EXCHANGE_SYMBOL_KEY = settings.USER_SUB_EXCHANGE_SYMBOL_KEY
# csv 上次读取位置记录
S_PAIR_READ_LINE_NO_KEY = settings.PAIR_READ_LINE_NO_KEY

WS_TYPE_TRADE = settings.BASE_WS_TYPE_TRADE
WS_TYPE_KLINE = settings.BASE_WS_TYPE_KLINE

S_IMPORT_DATA_LAST_MTIME_KEY = settings.IMPORT_DATA_LAST_MTIME_KEY

ENCODING = 'utf-8'

SPIDERS_KEY = 'spiders'
SYMBOLS_KEY = 'symbols'

STATUS_KEY = 's'
TMS_KEY = 't'
PID_KEY = 'p'


""" HASH field value 形态

# 初始化生成,  只有s
exchange_status = {
    'huobipro': 1,
    'ex1': 2,
    'ex2': 4,
    
}

# 加锁: 每次只能由一个spider 来绑定
# 启动脚本导入, 以后只更新状态,  停止 恢复1, pid归0, 
huobipro_symbols_status = {
    'btcusdt': {
        's': 1,
        't': 123,
        'pid': 1234
    }
}
# 查 所有symbol,  并绑定pid --> 只需设置一次

# 绑定 symbol 后设置, 启动 清空hdel, 每次更新只设置 s, t 
huobipro_spiders_status = {
    1234: {
        's': 1,
        't': 123,
        'symbols': [
            'btcusdt'
        ]
    }
}

"""

logger = get_logger(__name__, is_debug=settings.IS_DEBUG)

class Redis:
    def __init__(self, redis_config):
        connection_pool = redis.ConnectionPool(**redis_config)
        try:
            self.r = redis.Redis(connection_pool=connection_pool)
        except Exception as e:
            print("Failed to connect redis: ", e)

    def ping(self):
        print("Redis connection: ", self.r.ping())

# 非 协程 redis_conn
REDIS_CON = Redis(settings.LOCAL_REDIS_CONF).r


class LockTimeout(BaseException):
    pass


class RedisLock(object):

    def __init__(self, key, expires=20, timeout=10):
        """
        Distributed locking using Redis SETNX and GETSET.

        Usage::

            async with Lock('my_lock'):
                print "Critical section"

        :param  expires     We consider any existing lock older than
                            ``expires`` seconds to be invalid in order to
                            detect crashed clients. This value must be higher
                            than it takes the critical section to execute.
        :param  timeout     If another client has already obtained the lock,
                            sleep for a maximum of ``timeout`` seconds before
                            giving up. A value of 0 means we never wait.
        """

        self.key = key
        self.timeout = timeout
        self.expires = expires
        self.redis_conn = AsyncRedis()

    async def __aenter__(self):
        timeout = self.timeout
        while timeout >= 0:
            expires = time.time() + self.expires + 1

            if await (await self.redis_conn.redis).setnx(self.key, expires):
                # We gained the lock; enter critical section
                return

            current_value = await (await self.redis_conn.redis).get(self.key)

            # We found an expired lock and nobody raced us to replacing it
            if current_value and float(current_value) < time.time() and \
                    await (await self.redis_conn.redis).getset(self.key, expires) == current_value:
                return

            timeout -= 1
            await asyncio.sleep(1)

        raise LockTimeout("Timeout whilst waiting for lock")

    async def __aexit__(self, exc_type, exc_value, traceback):
        await (await self.redis_conn.redis).delete(self.key)


def redis_lock(key, expires, timeout):
    """
    功能:
        装饰器 获取 redis 锁后 进行后续操作
    """
    def get_args(func):
        async def wrapper(*args, **kwargs):
            # await logger.error(f'{func.__name__} > get redis lock, {key, expires, timeout}')
            while 1:
                try:
                    async with RedisLock(key, expires, timeout):
                        return await func(*args, **kwargs)
                except LockTimeout:
                    await asyncio.sleep(0.1)
                    continue
        return wrapper
    return get_args


class ExchangeSpiderError(BaseException):
    ...


class ExchangeSymbolError(BaseException):
    ...



class AsyncRedisBase:
    MIN_SIZE = 5
    MAX_SIZE = 20

    def __init__(self, loop=None, config=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self._config = config if config else settings.LOCAL_REDIS_CONF
        self.aioredis_url = self.get_aioredis_url()

    def get_aioredis_url(self):
        redis_conf = self._config
        aioredis_conf_url = f'{redis_conf["host"]}:{redis_conf["port"]}/{redis_conf["db"]}'
        if 'password' in redis_conf:
            aioredis_url = f'redis://:{redis_conf["password"]}@{aioredis_conf_url}'
        else:
            aioredis_url = f'redis://{aioredis_conf_url}'
        return aioredis_url

    @property
    async def redis(self):
        """
        功能:
            获取 协程redis 实例
        """
        global _ASYNC_REDIS_MAP
        if self.aioredis_url not in _ASYNC_REDIS_MAP or _ASYNC_REDIS_MAP[self.aioredis_url].closed:
            try:
                redis = await aioredis.create_redis_pool(
                    self.aioredis_url,
                    minsize=AsyncRedis.MIN_SIZE,
                    maxsize=AsyncRedis.MAX_SIZE,
                    loop=self.loop)
                _ASYNC_REDIS_MAP[self.aioredis_url] = redis
            except Exception as e:
                raise BaseException(f'redis connection error {e}')
        return _ASYNC_REDIS_MAP[self.aioredis_url]


class AsyncRedis(AsyncRedisBase):

    MIN_SIZE = 5
    MAX_SIZE = 20

    def __init__(self, loop=None, config=None):
        config = config if config else settings.LOCAL_REDIS_CONF
        super().__init__(loop, config)

    @property
    async def _now_tms(self):
        return int(time.time())

    async def get_exchange_spider_data(self, key: str, pid: str) -> dict:
        """
        功能:
            获取 spider data
        """
        cur = await self.redis
        r_data = await cur.hget(key, pid)
        if not r_data:
            data = {
                TMS_KEY: await self._now_tms,
                STATUS_KEY: SpiderStatus.pending.value,
                SYMBOLS_KEY: []
            }
            await cur.hset(key, pid, ujson.dumps(data))
            return data
        try:
            data = ujson.loads(r_data)
            return data
        except Exception as e:
            raise ExchangeSpiderError(f"{e}")

    async def set_exchange_spider_data(self, key: str, pid: str, data: dict) -> None:
        """
        功能:
            设置 spider data
        """
        cur = await self.redis
        try:
            if isinstance(data, dict):
                data = ujson.dumps(data)
            await cur.hset(key, pid, data)
        except Exception as e:
            raise ExchangeSpiderError(f"{e}")

    async def get_exchange_symbol_data(self, key: str, symbol: str) -> dict:
        """
        功能:
            获取 exchange symbols data
        """
        cur = await self.redis
        r_data = await cur.hget(key, symbol)
        try:
            data = ujson.loads(r_data)
            return data
        except Exception as e:
            return {}

    async def get_exchange_all_symbols_data(self, key: str) -> dict:
        cur = await self.redis
        all_symbols_data = await cur.hgetall(key)
        try:
            all_symbols_dict = {x.decode(): ujson.loads(all_symbols_data[x]) for x in all_symbols_data}
            return all_symbols_dict
        except Exception as e:
            raise ExchangeSymbolError(f'{e}')

    async def check_has_pending_symbols_by_pid(self, exchange_id: str, pid: str='', limit: int=0, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> bool:
        """
        功能:
            是否有 待启动交易对 开启 ws
        注意:
            有 limit 首次从 symbols中取 需要绑定
            有 pid: 直接从spider取
        """
        if limit:
            key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
            spider_key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
            all_symbols_dict = await self.get_exchange_all_symbols_data(key)
            pending_symbols = [x for x in all_symbols_dict if all_symbols_dict[x][STATUS_KEY] == SpiderStatus.pending.value
                               and not all_symbols_dict[x][PID_KEY]]
            pending_symbols = sorted(pending_symbols)[:limit]
            await self.hmset_exchange_symbol_status(exchange_id, pending_symbols, status=SpiderStatus.pending.value,
                                                    pid=pid, ws_type=ws_type)
            # 绑定到 spider上
            pid_data = {
                TMS_KEY: await self._now_tms,
                STATUS_KEY: SpiderStatus.pending.value,
                SYMBOLS_KEY: pending_symbols
            }
            await self.set_exchange_spider_data(spider_key, pid, pid_data)

        else:
            key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
            spider_data = await self.get_exchange_spider_data(key, pid)
            pending_symbols = spider_data[SYMBOLS_KEY]
        return True if pending_symbols else False

    async def get_pending_symbol_by_pid(self, exchange_id: str, limit: int=0, pid: str='', ws_type: str=WS_TYPE_TRADE) -> list:
        """
        功能:
            获取 待启动symbol
        """
        if pid:
            key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
            spider_data = await self.get_exchange_spider_data(key, pid)
            pending_symbols = spider_data[SYMBOLS_KEY]
        else:
            key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
            all_symbols_data = await self.get_exchange_all_symbols_data(key)
            pending_symbols = [x for x in all_symbols_data if all_symbols_data[x][STATUS_KEY] == SpiderStatus.pending.value
                               and not all_symbols_data[x][PID_KEY]]

        symbols = pending_symbols if not limit else pending_symbols[:limit]
        return symbols

    async def set_exchange_spider_status(self, exchange_id: str, pid: str, status: int, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE, symbols=None) -> None:
        """
        功能:
            设置 spider 和 交易所 状态
        """
        cur = await self.redis
        key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
        spider_data = await self.get_exchange_spider_data(key, pid)
        spider_data[STATUS_KEY] = status
        spider_data[TMS_KEY] = await self._now_tms
        spider_data[SYMBOLS_KEY] = symbols if symbols else []
        await cur.hset(key, pid, ujson.dumps(spider_data))

    async def add_exchange_spider_symbols(self, exchange_id: str, pid: str, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE, symbols: list=None) -> None:
        """
        功能:
            新增 spider 绑定的交易对
        """
        if not symbols:
            return
        cur = await self.redis
        key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
        spider_data = await self.get_exchange_spider_data(key, pid)
        spider_data[TMS_KEY] = await self._now_tms
        now_symbols = spider_data[SYMBOLS_KEY]
        now_symbols.extend(symbols)
        spider_data[SYMBOLS_KEY] = list(set(now_symbols))
        await cur.hset(key, pid, ujson.dumps(spider_data))

    async def delete_exchange_spider_symbols(self, exchange_id: str, pid: str, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE, symbols: list=None) -> None:
        """
        功能:
            删除 spider 绑定的交易对
        """
        if not symbols:
            return
        cur = await self.redis
        key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
        spider_data = await self.get_exchange_spider_data(key, pid)
        spider_data[TMS_KEY] = await self._now_tms
        now_symbols = spider_data[SYMBOLS_KEY]
        spider_data[SYMBOLS_KEY] = [x for x in now_symbols if x not in symbols]
        await cur.hset(key, pid, ujson.dumps(spider_data))

    async def set_exchange_symbol_status(self, exchange_id: str, symbol: str, status: int, pid: str='', ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> None:
        """
        功能:
            设置 symbol 运行中 状态 这里是单独设置的key 用来保存 symbol 状态
        """
        cur = await self.redis
        key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
        symbol_data = {
            TMS_KEY: await self._now_tms,
            STATUS_KEY: status,
            PID_KEY: pid
        }
        await cur.hset(key, symbol, ujson.dumps(symbol_data))

    async def hmset_exchange_symbol_status(self, exchange_id: str, symbols: list, status: int, pid: str='', ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> None:
        """
        功能:
            设置 多个  symbol 状态
        """
        if not symbols:
            return
        cur = await self.redis
        key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
        now = await self._now_tms
        update_data = {
            symbol: ujson.dumps({
                TMS_KEY: now,
                STATUS_KEY: status,
                PID_KEY: pid
            })
            for symbol in symbols
        }
        await cur.hmset_dict(key, update_data)

    async def set_pid_symbols_status(self, exchange_id: str, status: int, pid: str='', ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> list:
        """
        功能:
            设置 spider 所有关联交易对的 状态
        """
        symbols_list = []
        if not pid:
            return symbols_list
        spider_key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
        spider_data = await self.get_exchange_spider_data(spider_key, pid)
        symbols_list = spider_data[SYMBOLS_KEY]
        await self.hmset_exchange_symbol_status(exchange_id, symbols_list, status, pid, ws_type=ws_type)
        return symbols_list

    async def check_ex_spider_stop(self, exchange_id: str, spider_pid: str='', ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> bool:
        """
        功能:
            缓存 检查 spider 是否停止
        参数:
            spider_pid 是检查 交易所 还是交易所spider
        返回:
            ret: True 停止
        """
        cur = await self.redis
        # 交易所状态
        ex_data = await cur.hget(S_EXCHANGE_STATUS_KEY_MAP[ws_type], exchange_id)
        if not ex_data:
            raise BaseException(f'{exchange_id} not has redis data')
        if int(ex_data) == SpiderStatus.stopped.value:
            await logger.info(f'{exchange_id} Admin Set STOP...')
            return True

        # 交易所 spider 状态
        if not spider_pid:
            return False
        spider_key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
        spider_data = await self.get_exchange_spider_data(spider_key, spider_pid)
        if spider_data.get(STATUS_KEY, 0) in [SpiderStatus.stopped.value, SpiderStatus.error_stopped.value]:
            await logger.info(f'{exchange_id} {spider_pid} retry connection...')
            return True
        return False

    async def check_ex_symbol_stop(self, exchange_id: str, symbol: str, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> bool:
        """
        功能:
            是否停止symbol 抓取
        """
        key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
        symbol_data = await self.get_exchange_symbol_data(key, symbol)
        if not symbol_data:
            return True
        if symbol_data[STATUS_KEY] == SpiderStatus.stopped.value:
            return True
        return False

    async def close_exchange(self, exchange_id: str, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> bool:
        """
        功能:
            关闭交易所
        """
        try:
            cur = await self.redis
            # 关闭交易对
            now = await self._now_tms
            symbol_key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(exchange_id)
            symbols= [x.decode() for x in await cur.hkeys(symbol_key)]
            update_data = {
                symbol: ujson.dumps({
                    TMS_KEY: now,
                    STATUS_KEY: SpiderStatus.pending.value,
                    PID_KEY: ''
                })
                    for symbol in symbols
            }
            await cur.hmset_dict(symbol_key, update_data)
            # 清空spider
            spider_key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(exchange_id)
            await cur.delete(spider_key)
            # 关闭交易所
            await cur.hset(S_EXCHANGE_STATUS_KEY_MAP[ws_type], exchange_id, SpiderStatus.stopped.value)
            return True
        except Exception as e:
            await logger.error(e)
            return False

    async def cmd_start_exchange(self, exchange_id: str=None, symbol: str=None, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> None:
        """
        功能:
            启动所有交易所
        """
        try:
            status = SpiderStatus.running.value
            cur = await self.redis
            exchange_status_key = S_EXCHANGE_STATUS_KEY_MAP[ws_type]
            if exchange_id:
                await cur.hset(exchange_status_key, exchange_id, status)
                exchange_ids = [exchange_id]
            else:
                redis_ex_data = await cur.hkeys(exchange_status_key)
                if not redis_ex_data:
                    raise BaseException('redis has not exchange')
                all_exchanges = [x.decode() for x in redis_ex_data]
                update_dict = {
                    ex_id:
                        status
                    for ex_id in all_exchanges
                }
                await cur.hmset_dict(exchange_status_key, update_dict)
                exchange_ids = all_exchanges
            for ex_id in exchange_ids:
                now = await self._now_tms
                symbol_key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(ex_id)
                if symbol:
                    symbol_data = {
                        TMS_KEY: now,
                        STATUS_KEY: status,
                        PID_KEY: ''
                    }
                    await cur.hset(symbol_key, symbol, ujson.dumps(symbol_data))
                else:
                    symbols = [x.decode() for x in await cur.hkeys(symbol_key)]
                    update_data = {
                        symbol: ujson.dumps({
                            TMS_KEY: now,
                            STATUS_KEY: status,
                            PID_KEY: ''
                        })
                        for symbol in symbols
                    }
                    await cur.hmset_dict(symbol_key, update_data)
        except Exception as e:
            await logger.error(e)

    async def cmd_stop_exchange(self, exchange_id: str=None, symbol: str=None, ws_type: WS_TYPE_TRADE=WS_TYPE_TRADE) -> None:
        """
        功能:
            关闭交易所的交易对
        """
        try:
            status = SpiderStatus.stopped.value
            cur = await self.redis
            exchange_status_key = S_EXCHANGE_STATUS_KEY_MAP[ws_type]
            if exchange_id:
                await cur.hset(exchange_status_key, exchange_id, status)
                exchange_ids = [exchange_id]
            else:
                redis_ex_data = await cur.hkeys(exchange_status_key)
                if not redis_ex_data:
                    raise BaseException('redis has not exchange')
                all_exchanges = [x.decode() for x in redis_ex_data]
                update_dict = {
                    ex_id:
                        status
                    for ex_id in all_exchanges
                }
                await cur.hmset_dict(exchange_status_key, update_dict)
                exchange_ids = all_exchanges
            for ex_id in exchange_ids:
                now = await self._now_tms
                symbol_key = S_SYMBOL_STATUS_KEY_MAP[ws_type].format(ex_id)
                spider_key = S_SPIDER_STATUS_KEY_MAP[ws_type].format(ex_id)
                await cur.delete(spider_key)
                if symbol:
                    symbol_data = {
                        TMS_KEY: now,
                        STATUS_KEY: status,
                        PID_KEY: ''
                    }
                    await cur.hset(symbol_key, symbol, ujson.dumps(symbol_data))
                else:
                    symbols = [x.decode() for x in await cur.hkeys(symbol_key)]
                    update_data = {
                        symbol: ujson.dumps({
                            TMS_KEY: now,
                            STATUS_KEY: status,
                            PID_KEY: ''
                        })
                        for symbol in symbols
                    }
                    await cur.hmset_dict(symbol_key, update_data)
        except Exception as e:
            await logger.error(e)

    async def add_exchange_symbol_trades(self, exchange_id, symbol, trade_list):
        """
        功能:
            添加 trade 用于本地写csv
        """
        key = S_EXCHANGE_SYMBOL_CACHE_KEY_MAP[WS_TYPE_TRADE].format(exchange_id, symbol)
        cur = await self.redis
        try:
            await cur.rpush(key, *[ujson.dumps(x) for x in trade_list])
        except Exception as e:
            await logger.error(f'Redis rpush Trades Error: {e}')

    async def add_exchange_symbol_trades_2_ohlcv(self, exchange_id, symbol, trade_list):
        """
        功能:
            添加 trade 用于本地写csv
        """
        key = settings.EXCHANGE_SYMBOL_OHLCV_TRADE_KEY.format(exchange_id, symbol)
        cur = await self.redis
        try:
            await cur.rpush(key, *[ujson.dumps(x) for x in trade_list])
        except Exception as e:
            await logger.error(f'Redis rpush Trades Error: {e}')

    async def add_exchange_symbol_klines(self, exchange_id, symbol, klines):
        """
        功能:
            添加 kline 用于本地写csv
        """
        key = S_EXCHANGE_SYMBOL_CACHE_KEY_MAP[WS_TYPE_KLINE].format(exchange_id, symbol)
        cur = await self.redis
        try:
            await cur.rpush(key, *[ujson.dumps(kline) for kline in klines])
        except Exception as e:
            await logger.error(f'Redis rpush Trades Error: {e}')

    async def push_csv_direct(self, symbol_exchange, ws_type=WS_TYPE_TRADE):
        """
        功能:
            push_csv_direct
        参数:
            symbol_exchange @: btcusdt:huobipro
        """
        await logger.debug(f'{symbol_exchange} {ws_type} push_csv_direct...')
        key = S_PENDING_CSV_MAP[ws_type]
        await (await self.redis).rpush(key, symbol_exchange)

    async def update_exchange_last_ohlcv(self, exchange_id, symbol, ohlcv, time_frame):
        """
        功能:
            更新最新一条 kline 到缓存
        """
        cur = await self.redis
        key = S_EXCHANGE_LAST_OHLCV_KEY.format(exchange_id)
        field = f'{symbol}_{TimeFrame(time_frame).name}'
        await cur.hset(key, field, ujson.dumps(ohlcv))

    async def send_user_sub_pair(self, pair_dict):
        """
        功能:
            发送用户 订阅到spider
        """
        cur = await self.redis
        await cur.hmset_dict(S_USER_SUB_EXCHANGE_SYMBOL_KEY, pair_dict)

    async def get_all_user_sub_pair(self) -> dict:
        """
        功能:
            获取 当前所有用户关注的 交易对, 取消关注的del, 新增的hset
        """
        user_sub_key = S_USER_SUB_EXCHANGE_SYMBOL_KEY
        cur = await self.redis
        user_sub_datas = await cur.hgetall(user_sub_key, encoding=ENCODING)
        if not user_sub_datas:
            return {}
        else:
            now = await self._now_tms
            sub_pair_dict = {
                pair: int(user_sub_datas[pair])
                for pair in user_sub_datas
                if int(user_sub_datas[pair]) > (now - settings.USER_SUB_EX)
            }
            del_pair = [x for x in user_sub_datas if x not in sub_pair_dict]
            for pair in del_pair:
                await cur.hdel(user_sub_key, pair)
            return sub_pair_dict

    async def check_pair_is_user_sub(self, exchange_id, symbol) -> bool:
        """
        功能:
            检验交易对是否用户订阅
        """
        is_delete_key = False
        ret = False
        user_sub_key = S_USER_SUB_EXCHANGE_SYMBOL_KEY
        cur = await self.redis
        # 检测是否有用户订阅
        pair = f'{symbol}:{exchange_id}'
        pair_sub_tms = await cur.hget(user_sub_key, pair, encoding=ENCODING)
        if pair_sub_tms:
            if await self._now_tms - int(pair_sub_tms) > settings.USER_SUB_EX:
                await cur.hdel(user_sub_key, pair)
                is_delete_key = True
            else:
                ret = True
        else:
            is_delete_key = True
        if is_delete_key:
            await self.delete_ohlcv_trades_key(cur, exchange_id, symbol)
        return ret

    async def delete_ohlcv_trades_key(self, cur, exchange_id, symbol):
        """
        #TODO 上线kline 后 删除此代码
        """
        try:
            await cur.delete(settings.EXCHANGE_SYMBOL_OHLCV_TRADE_KEY.format(exchange_id, symbol))
        except:
            ...

    async def get_trade_list(self, exchange_id: str, symbol: str) -> list:
        trade_list = []
        key = S_EXCHANGE_SYMBOL_OHLCV_TRADE_KEY.format(exchange_id, symbol)
        cur = await self.redis
        for i in range(10):
            try:
                trade_data = await cur.lpop(key, encoding=ENCODING)
            except:
                trade_data = None
            if trade_data:
                trade_list.append(ujson.loads(trade_data))
        return trade_list if not trade_list else sorted(trade_list, key=lambda x: x[1])

    async def init_aicoin_pair_cache(self, map_dict: dict):
        """
        功能:
            添加aicoin 所有映射到缓存中
        """
        cur = await self.redis
        key = settings.AICOIN_PAIR_CACHE_KEY
        await cur.hmset_dict(key, map_dict)

    async def get_aicoin_pair_data(self, pair_ex):
        """
        功能:
            获取 pair 在aicoin 的请求参数
        """
        cur = await self.redis
        key = settings.AICOIN_PAIR_CACHE_KEY
        data = await cur.hget(key, pair_ex, encoding=ENCODING)
        if not data:
            return {}
        else:
            return ujson.loads(data)

    async def get_import_data_last_mtime(self, exchange_id, symbol, spider_name):
        """
        功能:
            获取导入1分钟csv文件最后的修改时间
        """
        try:
            cur = await self.redis
            key = S_IMPORT_DATA_LAST_MTIME_KEY
            field = f"""{spider_name}:{symbol}:{exchange_id}"""
            data = await cur.hget(key, field, encoding=ENCODING)
            if not data:
                return 0
            else:
                return float(data)
        except Exception as e:
            await logger.error("Exception to get_import_data_last_mtime(%s,%s). %s" % (exchange_id, symbol, e))
        return 0

    async def set_import_data_last_mtime(self, exchange_id, symbol, last_mtime, spider_name):
        """
        功能:
            设置导入1分钟csv文件最后的修改时间
        """
        try:
            cur = await self.redis
            key = S_IMPORT_DATA_LAST_MTIME_KEY
            field = f"""{spider_name}:{symbol}:{exchange_id}"""
            await cur.hset(key, field, last_mtime)
        except Exception as e:
            await logger.error("Exception to set_import_data_last_mtime(%s,%s). %s" % (exchange_id, symbol, e))


class StoreKeeperAsyncRedis(AsyncRedis):

    MIN_SIZE = 5
    MAX_SIZE = 20

    def __init__(self, loop=None, config=None):
        config = config if config else settings.STOPKEEPER_REDIS_CONFIG
        super().__init__(loop, config)

    async def set_pair_last_read_no(self, exchange_id, symbol, file_name, last_no, spider_name):
        """
        功能:
            设置 文件 最后读取位置
        """
        try:
            cur = await self.redis
            key = S_PAIR_READ_LINE_NO_KEY
            field = f"""{spider_name}:{symbol}:{exchange_id}"""
            value = f'{file_name}:{last_no}'
            await cur.hset(key, field, value)
        except Exception as e:
            await logger.error(e)

    async def get_pair_last_read_no(self, exchange_id, symbol, file_name, spider_name):
        """
        功能:
            获取 交易对 上次 读取位置
        """
        try:
            cur = await self.redis
            key = S_PAIR_READ_LINE_NO_KEY
            field = f"""{spider_name}:{symbol}:{exchange_id}"""
            data = await cur.hget(key, field, encoding=ENCODING)
            if not data:
                return 0
            else:
                last_file_name, last_no = data.split(':')
                if file_name == last_file_name:
                    return int(last_no)
                else:
                    return 0
        except Exception as e:
            await logger.error(e)
            return 0