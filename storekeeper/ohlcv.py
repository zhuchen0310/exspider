#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-04 19:30

"""
说明:
    k线 计算中心,  用户关注下

"""
import time
import ujson
import datetime
import asyncio
from collections import deque

from django.conf import settings

from exspider.utils.enum_con import TimeFrame
from spider import ws_crawl
from exspider.utils.pg_con import OHLCVPg
from exspider.utils.redis_con import StoreKeeperAsyncRedis

SOURCE_KEY = 's'
QUEUE_MAX_LEN_KEY = 'l'
TMS_KEY = 't'

# trade 计算1min kline缓存

TRADE_OHLCV_QUEUE_MAXLEN = 2

TRADE_OHLCV_CACHE = {
    # 's:ex': deque(maxlen=2),
}

# kline 计算映射 5min 根据 1min 计算
CAL_MAP = {
    TimeFrame.m1.value: {
        SOURCE_KEY: '',         # 计算数据来源
        QUEUE_MAX_LEN_KEY: 5    # 该timeframe 长度
    },
    TimeFrame.m5.value: {
        SOURCE_KEY: TimeFrame.m1.value,
        QUEUE_MAX_LEN_KEY: 3
    },
    TimeFrame.m15.value: {
        SOURCE_KEY: TimeFrame.m5.value,
        QUEUE_MAX_LEN_KEY: 2
    },
    TimeFrame.m30.value: {
        SOURCE_KEY: TimeFrame.m15.value,
        QUEUE_MAX_LEN_KEY: 2
    },
    TimeFrame.h1.value: {
        SOURCE_KEY: TimeFrame.m30.value,
        QUEUE_MAX_LEN_KEY: 4
    },
    TimeFrame.h4.value: {
        SOURCE_KEY: TimeFrame.h1.value,
        QUEUE_MAX_LEN_KEY: 3
    },
    TimeFrame.h12.value: {
        SOURCE_KEY: TimeFrame.h4.value,
        QUEUE_MAX_LEN_KEY: 2
    },
    TimeFrame.d1.value: {
        SOURCE_KEY: TimeFrame.h12.value,
        QUEUE_MAX_LEN_KEY: 31
    },
    TimeFrame.w1.value: {
        SOURCE_KEY: TimeFrame.d1.value,
        QUEUE_MAX_LEN_KEY: 1
    },
    TimeFrame.M1.value: {
        SOURCE_KEY: TimeFrame.d1.value,
        QUEUE_MAX_LEN_KEY: 1
    },
}

# 用户订阅的交易对缓存
USER_SUB_OHLCV_CACHE = {
    # 'btcusdt:huobipro': {
    #     'tms': int(time.time()),
    #     '1min': deque(maxlen=5),
    #     '5min': deque(maxlen=3),
    # }
}

# 当前用户有效订阅 pair
LAST_USER_SUB_PAIR = []


async def now():
    return int(time.time())


async def get_timeframe_tms(time_frame, time_stamp):
    """
    功能:
        初始化一条数据只是为了生成的数据一致性,  比如5分钟的那就是 8:00 | 8:05
    """
    if not (time_frame and time_stamp):
        return None
    struct_time = time.localtime(time_stamp)
    start_1month = int(
        datetime.datetime.strptime(
            f'{struct_time.tm_year}-{struct_time.tm_mon}-01 08:00:00', '%Y-%m-%d %H:%M:%S').timestamp())
    start_1week = time_stamp - time.localtime(time_stamp).tm_wday * 24 * 60 * 60
    default_ohlcv_data = {
        TimeFrame.m1.value: time_stamp - time_stamp % 60,
        TimeFrame.m3.value: time_stamp - time_stamp % (3 * 60),
        TimeFrame.m5.value: time_stamp - time_stamp % (5 * 60),
        TimeFrame.m10.value: time_stamp - time_stamp % (10 * 60),
        TimeFrame.m15.value: time_stamp - time_stamp % (15 * 60),
        TimeFrame.m30.value: time_stamp - time_stamp % (30 * 60),
        TimeFrame.h1.value: time_stamp - time_stamp % (60 * 60),
        TimeFrame.h2.value: time_stamp - time_stamp % (2 * 60 * 60),
        TimeFrame.h4.value: time_stamp - time_stamp % (4 * 60 * 60),
        TimeFrame.h6.value: time_stamp - time_stamp % (6 * 60 * 60),
        TimeFrame.h12.value: time_stamp - time_stamp % (12 * 60 * 60),
        TimeFrame.d1.value: time_stamp - time_stamp % (24 * 60 * 60),
        TimeFrame.d3.value: time_stamp - time_stamp % (3 * 24 * 60 * 60),
        TimeFrame.w1.value: start_1week - start_1week % (24 * 60 * 60),
        TimeFrame.M1.value: start_1month
    }
    if time_frame not in default_ohlcv_data:
        return None
    return default_ohlcv_data[time_frame]


class OHLCV:
    """
    说明:
        交易所 通过trade 计算交易对 kline 的类
        初始 kline 全部从aicoin 获取
    """

    def __init__(self, loop):
        self.ex = getattr(ws_crawl, settings.BASE_AI_COIN_EXCHANGE_ID)(loop)
        self.init_kline_cache = {
            #'btcusdt:huobipro': True 是否需要请求restful
        }
        self.store_redis = StoreKeeperAsyncRedis(loop)

    async def start_care_user_sub_pair(self):
        """
        功能:
            生产者 持续获取当前有用户订阅的pair
        """
        global LAST_USER_SUB_PAIR
        print('Start 订阅监控!')
        while True:
            # 现在从store_redis 中取用户关注, 然后 发到spider
            all_user_sub_pair = await self.store_redis.get_all_user_sub_pair()
            if all_user_sub_pair:
                await self.ex.redis_con.send_user_sub_pair(all_user_sub_pair)
            LAST_USER_SUB_PAIR = list(all_user_sub_pair.keys()) if all_user_sub_pair else []
            await asyncio.sleep(1)

    async def start_get_ex_trades_forever(self):
        """
        功能:
            消费者 持续从远程缓存 为订阅pair 获取最新trade数据
        """
        print('Start 持续计算!')
        await asyncio.sleep(5)
        while True:
            # 删除掉 过期的pair
            cur = await self.ex.redis_con.redis
            for pair in list(USER_SUB_OHLCV_CACHE.keys()):
                if pair not in LAST_USER_SUB_PAIR:
                    await self.close_pair_ex_ohlcv(pair)
            task = [asyncio.ensure_future(self.handel_one_pair_trade(cur, pair)) for pair in LAST_USER_SUB_PAIR]
            await asyncio.gather(*task)
            await asyncio.sleep(0.001)

    async def handel_one_pair_trade(self, cur, pair):
        symbol, exchange_id = pair.split(':')
        key = settings.EXCHANGE_SYMBOL_OHLCV_TRADE_KEY.format(exchange_id, symbol)
        trade_data_list = await cur.lrange(key, 0, 100, encoding='utf-8')
        if trade_data_list:
            await cur.ltrim(key, len(trade_data_list), -1)
            trade_list = (ujson.loads(data) for data in trade_data_list)
            await self.get_1min_kline_by_trade(exchange_id, symbol, trade_list)

    async def get_1min_kline_by_trade(self, exchange_id, symbol, trade_list):
        """
        功能:
            根据 trade 生成 1min k线
        格式:
            ohlcv = [
                t,
                o,
                h,
                l,
                c,
                v
            ]
            deque = (ohlcv1, ohlcv2; max_len=2)
        """
        s_ex = f'{symbol}:{exchange_id}'
        if s_ex not in TRADE_OHLCV_CACHE:
            # 首次 需要请求 restful
            while 1:
                kline_data = await self.ex.get_restful_klines(s_ex, TimeFrame.m1.value)
                if kline_data:
                    break
                await asyncio.sleep(1)
            async with OHLCVPg(exchange_id) as pg:
                await pg.insert_many(f'{symbol}_{TimeFrame.m1.value}', kline_data[:-1])
            kline_default = kline_data[-1:]
            TRADE_OHLCV_CACHE[s_ex] = deque(kline_default, maxlen=TRADE_OHLCV_QUEUE_MAXLEN)

        for trade in trade_list:
            ohlcv_deque = TRADE_OHLCV_CACHE[s_ex]
            last_ohlcv = ohlcv_deque[-1]
            last_tms = last_ohlcv[0]
            trade_tms = trade[0]    # 如果当前trade tms 小于 初始化的tms 直接返回
            if trade_tms < last_tms:
                print(trade_tms, last_tms)
                return
            tms = await get_timeframe_tms(TimeFrame.m1.value, trade_tms)    # 如果当前时间戳整点 小于 初始化则返回
            if tms < last_tms:
                return
            price = float(trade[3])
            volume = float(trade[4])
            # update
            if tms == last_tms:
                if price > last_ohlcv[2]:
                    last_ohlcv[2] = price
                if price < last_ohlcv[3]:
                    last_ohlcv[3] = price
                last_ohlcv[4] = price
                last_ohlcv[5] += volume
                ohlcv_deque[-1] = last_ohlcv
            elif tms > last_tms:
                new_ohlcv = [tms, price, price, price, price, volume]
                ohlcv_deque.append(new_ohlcv)
            await self.handel_all_timeframe_new_ohlcv(exchange_id, symbol, ohlcv_deque[-1])
        # print(f'{exchange_id} {symbol} 1min, last ohlcv:')
        # print(USER_SUB_OHLCV_CACHE[s_ex]['1min'][-1])
        # print()

    async def handel_all_timeframe_new_ohlcv(self, exchange_id, symbol, ohlcv_1min):
        """
        功能:
            根据最新的一条1分钟的ohlcv 计算其他最新的
        """
        await self.init_pair_ex_ohlcv(exchange_id, symbol)
        m1  = TimeFrame.m1.value
        await self.update_last_ohlcv(exchange_id, symbol, ohlcv_1min, timeframe=m1)
        task = [asyncio.ensure_future(self.calculation_one_timeframe_new_ohlcv(
            exchange_id, symbol, timeframe)) for timeframe in CAL_MAP if timeframe != m1]
        await asyncio.gather(*task)

    async def init_pair_ex_ohlcv(self, exchange_id, symbol) -> bool:
        """
        功能:
            初始化 ohlcv
        """
        pair_ex = f'{symbol}:{exchange_id}'
        if pair_ex not in self.init_kline_cache:
            print(f'Init {pair_ex} @ {datetime.datetime.now()}')
            self.init_kline_cache[pair_ex] = True
        if self.init_kline_cache.get(pair_ex, False):
            self.init_kline_cache[pair_ex] = False
            if pair_ex in USER_SUB_OHLCV_CACHE:
                return False
            if pair_ex not in USER_SUB_OHLCV_CACHE:
                USER_SUB_OHLCV_CACHE[pair_ex] = {TMS_KEY: await now()}
            for timeframe in CAL_MAP:
                print('>', timeframe)
                while 1:
                    try:
                        kline_data = await self.ex.get_restful_klines(pair_ex, timeframe)
                        if kline_data:
                            async with OHLCVPg(exchange_id) as pg:
                                await pg.insert_many(f'{symbol}_{timeframe}', kline_data[:-1])
                        kline_default = kline_data[-CAL_MAP[timeframe][QUEUE_MAX_LEN_KEY]:]
                    except:
                        kline_default = []
                    if kline_default:
                        break
                    await asyncio.sleep(1)
                deque_ohlcv = sorted(kline_default, key=lambda x: x[0])
                USER_SUB_OHLCV_CACHE[pair_ex].update({
                    timeframe: deque(deque_ohlcv, maxlen=CAL_MAP[timeframe][QUEUE_MAX_LEN_KEY])
                })
                await self.store_redis.update_exchange_last_ohlcv(exchange_id, symbol, kline_default[-1], timeframe)
            return True
        else:
            return False

    async def close_pair_ex_ohlcv(self, pair_ex):
        """
        功能:
            取消用户订阅
        """
        global USER_SUB_OHLCV_CACHE, TRADE_OHLCV_CACHE
        try:
            if pair_ex in self.init_kline_cache:
                self.init_kline_cache.pop(pair_ex)
            if pair_ex in USER_SUB_OHLCV_CACHE:
                USER_SUB_OHLCV_CACHE.pop(pair_ex)
            if pair_ex in TRADE_OHLCV_CACHE:
                TRADE_OHLCV_CACHE.pop(pair_ex)
            print(f'Closed {pair_ex} @ {datetime.datetime.now()}')
        except Exception as e:
            print(e)

    async def calculation_one_timeframe_new_ohlcv(self, exchange_id, symbol, timeframe):
        """
        功能:
            计算一个timeframe 最新k线
        """
        pair_ex = f'{symbol}:{exchange_id}'
        source_timeframe = CAL_MAP[timeframe][SOURCE_KEY]
        source_ohlcv_queue = USER_SUB_OHLCV_CACHE[pair_ex][source_timeframe]
        timestamp = await get_timeframe_tms(timeframe, source_ohlcv_queue[-1][0])
        can_use_ohlcv_queue = [x for x in source_ohlcv_queue if x[0] >= timestamp]
        if not can_use_ohlcv_queue:
            return
        o = can_use_ohlcv_queue[0][1]
        h = max([x[2] for x in can_use_ohlcv_queue])
        l = min([x[3] for x in can_use_ohlcv_queue])
        c = can_use_ohlcv_queue[-1][4]
        v = sum([x[5] for x in can_use_ohlcv_queue])
        await self.update_last_ohlcv(exchange_id, symbol, [timestamp, o, h, l, c, v], timeframe)

    async def update_last_ohlcv(self, exchange_id, symbol, ohlcv, timeframe=TimeFrame.m1.value):
        """
        功能:
            更新user_sub_ohlcv_map 缓存 中数据
            user_sub_ohlcv_map = {
                'btc_usdt': {
                    'timestamp': int(time.time()),
                    '1min': deque(maxlen=5),
                    '5min': deque(maxlen=3),
                    '15min': deque(maxlen=2),
                    '30min': deque(maxlen=2),
                }
            }
        操作:
            新的时间戳 与 当前缓存最后一条时间戳对比
            大于 -> 新增,
            等于 -> 更改最后一条,
            否则不处理
            处理后添加到redis
        """
        if not ohlcv:
            return
        # 操作 ohlcv
        pair_ex = f'{symbol}:{exchange_id}'
        now_tms = ohlcv[0]
        last_tms = USER_SUB_OHLCV_CACHE[pair_ex][timeframe][-1][0]
        if now_tms < last_tms:
            return
        elif now_tms > last_tms:
            async with OHLCVPg(exchange_id) as pg:
                await pg.insert_many(f'{symbol}_{timeframe}', [USER_SUB_OHLCV_CACHE[pair_ex][timeframe][-1]])
            USER_SUB_OHLCV_CACHE[pair_ex][timeframe].append(ohlcv)
        else:
            USER_SUB_OHLCV_CACHE[pair_ex][timeframe][-1] = ohlcv
        await self.store_redis.update_exchange_last_ohlcv(exchange_id, symbol, ohlcv, timeframe)


