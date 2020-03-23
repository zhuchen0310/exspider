# -*- coding:utf-8 -*-
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
from django.conf import settings
import tracemalloc

logger = get_logger(__name__, is_debug=settings.IS_DEBUG)


# 周期对应的分钟数值
PERIOD_INTERVAL_MAP = {
    TimeFrame.m1.value: 1,
    TimeFrame.m3.value: 3,
    TimeFrame.m5.value: 5,
    TimeFrame.m15.value: 15,
    TimeFrame.m30.value: 30,
    TimeFrame.h1.value: 60,
    TimeFrame.h2.value: 120,
    TimeFrame.h3.value: 180,
    TimeFrame.h4.value: 240,
    TimeFrame.h6.value: 360,
    TimeFrame.h12.value: 720,
    TimeFrame.d1.value: 1440,
    TimeFrame.d2.value: 2880,
    TimeFrame.d3.value: 4320,
    TimeFrame.d5.value: 7200,
    TimeFrame.w1.value: 10080,
    TimeFrame.M1.value: 43200,
    TimeFrame.Y1.value: 518400
}

# REALTIME_CALC_TIMEFRAME_LIST = [
#     TimeFrame.m5.value, TimeFrame.m15.value, TimeFrame.m30.value, TimeFrame.h1.value,
#     TimeFrame.h4.value, TimeFrame.h12.value, TimeFrame.d1.value, TimeFrame.w1.value, TimeFrame.M1.value
# ]
REALTIME_CALC_TIMEFRAME_LIST = [
    TimeFrame.m15.value, TimeFrame.h1.value, TimeFrame.d1.value
]


class StoreData(object):
    def __init__(self, server_id, mq_url, routing_key, loop):
        self._server_id = server_id
        self._mq_url = mq_url
        self._routing_key = routing_key
        self._loop = loop
        self._store_redis = StoreKeeperAsyncRedis(loop)
        self._bar_dict = {}  # from trades to kline
        self._ohlcv_pg_dict = {}  # exchange -> pg
        self._trades_pg_dict = {}  # exchange -> pg
        self._prev_exchange_symbol_last_ohlcv_cache = {}  # exchange_symbol_timeframe -> last ohlcv
        self._exchange_symbol_last_ohlcv_cache = {}  # exchange_symbol_timeframe -> last ohlcv, v 值是累加的，last缓存作为 base 使用
        self._exchange_symbol_history_ohlcv_need_update = set()  # exchange_symbol
        self._print_log = False
        self._print_log_last_time = time.time()
        self._server_start_time = time.time()
        self._last_statis_time = time.time()
        self._proc_msg_total = {'all': 0, 'kline': 0, 'trade': 0}
        self._exises_tables = {}  # exchange_id -> table name
        self._inited_ohlcv_pg_exchanges = set()
        self._last_min_end_time_dict = {}  # exchange_symbol -> last end time

    async def get_ohlcv_pg(self, exchange_id, time_frame):
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

    async def set_history_ohlcv_need_update(self, exchange, symbol):
        # 设置 exchange_symbol 的历史数据需要更新
        exchange_symbol = f"""{exchange}_{symbol}"""
        self._exchange_symbol_history_ohlcv_need_update.add(exchange_symbol)

    async def reset_history_ohlcv_need_update(self, exchange, symbol):
        # 重置缓存的历史数据，下次计算时，发现缓存没有，会重新从数据库查询
        exchange_symbol = f"""{exchange}_{symbol}"""
        if exchange_symbol in self._exchange_symbol_history_ohlcv_need_update:
            for time_frame in REALTIME_CALC_TIMEFRAME_LIST:
                last_ohlcv_cache_key = f"""{exchange}_{symbol}_{time_frame}"""
                if last_ohlcv_cache_key in self._prev_exchange_symbol_last_ohlcv_cache:
                    self._prev_exchange_symbol_last_ohlcv_cache.pop(last_ohlcv_cache_key)
            self._exchange_symbol_history_ohlcv_need_update.remove(exchange_symbol)

    async def msg_process_func(self, routing_key, data):
        self._proc_msg_total['all'] += 1
        # market.ticker.exhange.symbol.market_type.market_subtype.incr
        routing_key_words = routing_key.split(".")
        data_json = ujson.loads(data)
        if data_json['s'] in ['btcusdt', 'btcusd']:
            if time.time() - self._print_log_last_time > 10:  # 10秒打印一次日志
                self._print_log = True
                self._print_log_last_time = time.time()
                await logger.error("Received message %s from routing_key: %s." % (data, routing_key))
            else:
                self._print_log = False

        if 'kline' == routing_key_words[1]:
            if '1' == routing_key_words[-1] or '0' == routing_key_words[-1]:
                exchange_symbol = f"""{data_json['e']}_{data_json['s']}"""
                if exchange_symbol in self._last_min_end_time_dict:
                    last_min_end_time = self._last_min_end_time_dict[exchange_symbol]
                else:
                    last_min_end_time = 0
                ts = data_json['d'][0]
                if ts < last_min_end_time:
                    if "btcusdt" == data_json['s']:
                        await logger.error("Receive error btc/usdt kline, the current timestamp %s is litter last %s." % (ts, last_min_end_time))
                    if '1' == routing_key_words[-1]:
                        await self.insert_ohlcv(TimeFrame.m1.value, data_json)
                    return  # 直接丢掉错误时间戳的k线数据
                if '1' == routing_key_words[-1]:
                    self._last_min_end_time_dict[exchange_symbol] = ts + 1
                else:
                    self._last_min_end_time_dict[exchange_symbol] = ts
                # await logger.debug("Receive btc/usdt kline: timestamp=%s(%s), incr=%s." % (
                # datetime.datetime.fromtimestamp(ts), ts, routing_key_words[-1]))
            self._proc_msg_total['kline'] += 1
            if '1' == routing_key_words[-1]:  # full data, insert database
                await self.insert_ohlcv(TimeFrame.m1.value, data_json)
                await self.handel_all_timeframe_new_ohlcv(data_json['e'], data_json['s'], data_json['d'], insert_db=False)
            elif '-1' == routing_key_words[-1]:  # history full data:
                await self.insert_ohlcv(TimeFrame.m1.value, data_json)
                # 需要更新 -> 重置数据（现有缓存数据不可用)
                await self.set_history_ohlcv_need_update(data_json['e'], data_json['s'])
            elif '0' == routing_key_words[-1]:
                await self.reset_history_ohlcv_need_update(data_json['e'], data_json['s'])
                await self.handel_all_timeframe_new_ohlcv(data_json['e'], data_json['s'], data_json['d'], insert_db=False)
        elif 'trade' == routing_key_words[1]:
            self._proc_msg_total['trade'] += 1
            if routing_key_words[2] not in settings.PROVIDED_KLINE_EXCHANGES:
                for trades in data_json['d']:  # data_json['d'] maybe is [[]]
                    await self._update_minute_bar(data_json['e'], data_json['s'], trades)
            # 更新Trades数据：trades不写入数据库，在csv保存了。
            # await self.insert_trades(data_json)
        if time.time() - self._last_statis_time > 60:
            self._last_statis_time = time.time()
            interval = time.time() - self._server_start_time
            all_per_sec = self._proc_msg_total['all'] / interval
            kline_per_sec = self._proc_msg_total['kline'] / interval
            trade_per_sec = self._proc_msg_total['trade'] / interval
            await logger.debug("Number of messages processed per second: all=%.2f, kline=%.2f, trade=%.2f." % (
                all_per_sec, kline_per_sec, trade_per_sec))
            # snapshot = tracemalloc.take_snapshot()
            # top_stats = snapshot.statistics('lineno')
            # print("[ Top 10 ]")
            # for stat in top_stats[:10]:
            #     print(stat)
            # # exit(0)
            # tracemalloc.start()

    async def start(self):
        # tracemalloc.start()
        queue_name = 'store_keeper_%s' % self._server_id
        consumer = AioPikaConsumer(self._mq_url, self._loop, msg_process_func=self.msg_process_func,
                                   queue_name=queue_name, routing_keys=self._routing_key)
        await consumer.start()

    async def _update_minute_bar(self, exchange, symbol, trades):
        # 更新分钟线数据
        inner_symbol = exchange + "." + symbol
        trades_datetime = datetime.datetime.fromtimestamp(trades[0])  # trades timestamp is sec
        bar = None
        bar_datetime = None
        if inner_symbol not in self._bar_dict:
            # inner_symbol -> ([bar_datetime, volume, value], bar_datetime)
            self._bar_dict[inner_symbol] = ([None, 0.0, 0.0, 0.0, 0.0, 0.0], None)
        bar, bar_datetime = self._bar_dict[inner_symbol]

        ts_min = int(trades[0] / 60) * 60
        trade_price = trades[3]
        trade_volume = trades[4]
        # 如果第一个 TRADES 或者新的一分钟
        if not bar_datetime or bar_datetime.minute != trades_datetime.minute:
            if bar[0] is not None:
                new_bar = copy.copy(bar)
                # insert data to database
                await self.insert_ohlcv(TimeFrame.m1.value, {'c': 'kline',  'e': exchange, 's': symbol,  'd': new_bar})
                await self.handel_all_timeframe_new_ohlcv(exchange, symbol, new_bar, insert_db=True)
            # timestamp, o, h, l, c, v
            bar = [ts_min, trade_price, trade_price, trade_price, trade_price, trade_volume]
            self._bar_dict[inner_symbol] = (bar, trades_datetime)
        else:  # 否则继续累加新的K线
            bar[2] = max(bar[2], trade_price)  # high
            bar[3] = min(bar[3], trade_price)  # low
            bar[4] = trade_price
            bar[5] = float(bar[5]) + float(trade_volume)
        await self.reset_history_ohlcv_need_update(exchange, symbol)
        await self.handel_all_timeframe_new_ohlcv(exchange, symbol, bar)

    async def handel_all_timeframe_new_ohlcv(self, exchange_id, symbol, ohlcv_1min, insert_db=False):
        """
        功能:
            根据最新的一条1分钟的ohlcv 计算其他最新的
        """
        # await logger.debug("handel_all_timeframe_new_ohlcv: %s, %s, %s." % (exchange_id, symbol, ohlcv_1min))
        if not insert_db:
            timeframe = TimeFrame.m1.value
            last_ohlcv_cache_key = f"""{exchange_id}_{symbol}_{timeframe}"""
            if last_ohlcv_cache_key in self._exchange_symbol_last_ohlcv_cache:
                last_ohlcv = self._exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key]
                if last_ohlcv is not None and ohlcv_1min[0] > last_ohlcv[0]:
                    if symbol in ['btcusdt', 'btcusd']:
                        await logger.debug("insert last ohlcv: %s" % last_ohlcv)
                    await self.handel_all_timeframe_new_ohlcv(exchange_id, symbol, last_ohlcv, insert_db=True)

        await self.update_last_ohlcv(exchange_id, symbol, ohlcv_1min, timeframe=TimeFrame.m1.value)

        tbl_name = f'{symbol}_{TimeFrame.m1.value}'
        if exchange_id not in self._exises_tables:
            pg = await self.get_ohlcv_pg(exchange_id, TimeFrame.m1.value)
            await pg.is_exises_table(tbl_name)
            self._exises_tables[exchange_id] = set(tbl_name)  # init
        else:
            if tbl_name not in self._exises_tables[exchange_id]:
                pg = await self.get_ohlcv_pg(exchange_id, TimeFrame.m1.value)
                await pg.is_exises_table(tbl_name)
                self._exises_tables[exchange_id].add(tbl_name)

        if exchange_id not in self._inited_ohlcv_pg_exchanges:
            for timeframe in REALTIME_CALC_TIMEFRAME_LIST:
                await self.get_ohlcv_pg(exchange_id, timeframe)
            self._inited_ohlcv_pg_exchanges.add(exchange_id)
        # for time_frame in REALTIME_CALC_TIMEFRAME_LIST:
        #     await self.calculation_one_timeframe_new_ohlcv(exchange_id, symbol, time_frame, ohlcv_1min, insert_db)
        task = [asyncio.ensure_future(self.calculation_one_timeframe_new_ohlcv(
            exchange_id, symbol, timeframe, ohlcv_1min, insert_db)) for timeframe in REALTIME_CALC_TIMEFRAME_LIST]
        await asyncio.gather(*task)

    async def calculation_one_timeframe_new_ohlcv(self, exchange_id, symbol, timeframe, last_ohlcv_1min, insert_db):
        """
        功能:
            计算一个timeframe 最新k线
        """
        # await logger.debug("calculation_one_timeframe_new_ohlcv: %s, %s, %s." % (exchange_id, symbol, timeframe))
        now_tms = last_ohlcv_1min[0]
        now_min_time = int(now_tms / 60) * 60  # 当前的分钟时间
        # 1555903568 -> 2019/4/22 11:26:08
        # 5分钟，查1分钟，并且要从对齐位置开始计算 -> 2019/4/22 11:25:00
        period_start_time = int(now_tms / (PERIOD_INTERVAL_MAP[timeframe] * 60)) * PERIOD_INTERVAL_MAP[timeframe] * 60
        period_cur_time = int(now_tms / (PERIOD_INTERVAL_MAP[timeframe] * 60)) * PERIOD_INTERVAL_MAP[timeframe] * 60
        period_end_time_min = period_start_time + (PERIOD_INTERVAL_MAP[timeframe] - 1) * 60  # 本周期结束的最后分钟时间
        source_timeframe = TimeFrame.m1.value
        # if PERIOD_INTERVAL_MAP[timeframe] <= PERIOD_INTERVAL_MAP[TimeFrame.h1.value]:
        #     source_timeframe = TimeFrame.m1.value
        # elif PERIOD_INTERVAL_MAP[timeframe] <= PERIOD_INTERVAL_MAP[TimeFrame.d1.value]:
        #     source_timeframe = TimeFrame.h1.value  # 前面几个小时累计 + 最后的1个小时（60分钟）实时计算
        # else:
        #     source_timeframe = TimeFrame.d1.value  # 前面几天累计 + 最后的1天（24小时）实时计算
        # 前面的数据累计 + base实时数据计算
        last_ohlcv_cache_key = f"""{exchange_id}_{symbol}_{timeframe}"""
        base_last_ohlcv_cache_key = f"""{exchange_id}_{symbol}_{source_timeframe}"""
        base_last_ohlcv = self._exchange_symbol_last_ohlcv_cache[base_last_ohlcv_cache_key]
        if last_ohlcv_cache_key not in self._exchange_symbol_last_ohlcv_cache:
            self._exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key] = [0, 0, 0, 0, 0, 0]
        new_last_ohlcv = self._exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key]
        if last_ohlcv_cache_key not in self._prev_exchange_symbol_last_ohlcv_cache:
            prev_ohlcv = await self.get_history_ohlcv(exchange_id, symbol, source_timeframe,
                                                      period_start_time, base_last_ohlcv=base_last_ohlcv)
            self._prev_exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key] = prev_ohlcv
        else:
            prev_ohlcv = self._prev_exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key]
            if prev_ohlcv is None or prev_ohlcv[0] != period_start_time:  # 属于不同的开始时间，重新查数据库
                # prev_ohlcv = await self.get_history_ohlcv(exchange_id, symbol, source_timeframe,
                #                                           period_start_time, base_last_ohlcv=base_last_ohlcv)
                if prev_ohlcv and prev_ohlcv[0] != period_start_time and new_last_ohlcv[0] != 0:
                    # 开启了不同的时间段，则先把前面的数据入库
                    if TimeFrame.m15.value == timeframe and symbol in ['btcusdt', 'btcusd']:
                        await logger.debug("insert_ohlcv: %s" % copy.copy(new_last_ohlcv))
                    await self.insert_ohlcv(timeframe, {'c': 'kline', 'e': exchange_id, 's': symbol,
                                                        'd': copy.copy(new_last_ohlcv)})
                self._prev_exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key] = None
        if prev_ohlcv is None:
            new_last_ohlcv[0] = period_start_time
            new_last_ohlcv[1] = base_last_ohlcv[1]
            new_last_ohlcv[2] = base_last_ohlcv[2]
            new_last_ohlcv[3] = base_last_ohlcv[3]
            new_last_ohlcv[4] = base_last_ohlcv[4]
            new_last_ohlcv[5] = base_last_ohlcv[5]
            # new_last_ohlcv = [period_start_time, base_last_ohlcv[1], base_last_ohlcv[2],
            #                   base_last_ohlcv[3], base_last_ohlcv[4], base_last_ohlcv[5]]
            if TimeFrame.m15.value == timeframe and symbol in ['btcusdt', 'btcusd']:
                await logger.debug("%s, %s, prev volume: None, base volume: %s, new last volume: %s." % (
                    symbol, now_tms, base_last_ohlcv[5], new_last_ohlcv[5]))
        else:
            new_last_ohlcv[0] = period_start_time
            new_last_ohlcv[1] = prev_ohlcv[1]
            if 0 == float(prev_ohlcv[5]) and float(base_last_ohlcv[5]) != 0:
                new_last_ohlcv[1] = base_last_ohlcv[1]  # 当前面的成交量一直为0，直到有成交时，修正当前时间范围的开盘价
            new_last_ohlcv[2] = max(prev_ohlcv[2], base_last_ohlcv[2])
            new_last_ohlcv[3] = min(prev_ohlcv[3], base_last_ohlcv[3])
            new_last_ohlcv[4] = base_last_ohlcv[4]
            new_last_ohlcv[5] = float(prev_ohlcv[5]) + float(base_last_ohlcv[5])
            # new_last_ohlcv = [period_start_time, prev_ohlcv[1], max(prev_ohlcv[2], base_last_ohlcv[2]),
            #                   min(prev_ohlcv[3], base_last_ohlcv[3]), base_last_ohlcv[4],
            #                   float(prev_ohlcv[5]) + float(base_last_ohlcv[5])]
            if TimeFrame.m15.value == timeframe and symbol in ['btcusdt', 'btcusd']:
                await logger.debug("%s, %s, prev volume: %s, base volume: %s, new last volume: %s." % (
                    symbol, now_tms, prev_ohlcv[5], base_last_ohlcv[5], new_last_ohlcv[5]))
        await self.update_last_ohlcv(exchange_id, symbol, new_last_ohlcv, timeframe)
        if insert_db:
            # update prev ohlcv cache
            self._prev_exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key] = copy.copy(new_last_ohlcv)
            if TimeFrame.m15.value == timeframe and symbol in ['btcusdt', 'btcusd']:
                await logger.debug("%s: full kline: timeframe=%s, now_tms=%s, %s, now_min_time=%s, "
                                   "period_start_time=%s, period_end_time_min=%s." % (symbol, timeframe, now_tms,
                                                                                      datetime.datetime.fromtimestamp(now_tms),
                                                                                      now_min_time, period_start_time,
                                                                                      period_end_time_min))
            if now_min_time == period_end_time_min:
                if TimeFrame.m15.value == timeframe and symbol in ['btcusdt', 'btcusd']:
                    await logger.debug("insert_ohlcv: %s" % copy.copy(new_last_ohlcv))
                await self.insert_ohlcv(timeframe, {'c': 'kline', 'e': exchange_id, 's': symbol,
                                                    'd': copy.copy(new_last_ohlcv)})
                self._prev_exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key] = None

    async def update_last_ohlcv(self, exchange_id, symbol, ohlcv, timeframe=TimeFrame.m1.value):
        if not ohlcv:
            return
        # await logger.debug("update_last_ohlcv: %s, %s, %s." % (exchange_id, symbol, timeframe))
        last_ohlcv_cache_key = f"""{exchange_id}_{symbol}_{timeframe}"""
        self._exchange_symbol_last_ohlcv_cache[last_ohlcv_cache_key] = ohlcv
        await self._store_redis.update_exchange_last_ohlcv(exchange_id, symbol, ohlcv, timeframe)

    async def get_history_ohlcv(self, exchange_id, symbol, source_timeframe, period_start_time, base_last_ohlcv):
        ohlcv_list = await self.query_ohlcv(exchange_id, symbol, source_timeframe, from_tms=period_start_time)
        if 0 == len(ohlcv_list):
            last_ohlcv = None
        else:
            o = ohlcv_list[0][1]
            h = max([x[2] for x in ohlcv_list])
            l = min([x[3] for x in ohlcv_list])
            c = ohlcv_list[-1][4]
            v = sum([x[5] for x in ohlcv_list])
            last_ohlcv = [period_start_time, o, h, l, c, v]
        return last_ohlcv


