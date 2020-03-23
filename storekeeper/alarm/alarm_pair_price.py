#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 11:06

"""
文档: http://wiki.oa.ihold.com/pages/viewpage.action?pageId=11634122

功能:
    监控交易对价格变动

routing_key:
    "market.kline.okex.btc.usdt.spot.0.0"

data:
    'c': 'kline',                                                                   # channel
    'e': 'huobipro',                                                                # 交易所id
    't': 'timestamp',                                                               # 时间戳
    's': 'btcusdt',                                                                 # 交易对
    'd': [1555289520, 5131.96, 5134.23, 5131.96, 5133.76, 10.509788169770346]

"""
import ujson
import time

from django.conf import settings

from storekeeper.alarm.base import (
    BaseQueue, MergeQueue, PricePass, S_PRICE_ALARM_COIN_LIST, S_PASS_PRICE_COIN_LIST,
    SleepMergeQueue,
    get_exchange_pair_data,
    PairAlarmBase, PairCache
)

PAIR_PRICE_CACHE = {

}

PAIR_PRICE_PASS_CACHE = {

}

class PairAlarm(PairAlarmBase):

    """
    说明:
        价格 异动 类
    """

    async def alarm_main(self, routing_key, data, kline):
        # "market.kline.okex.btc.usdt.spot.0.0"
        _split_routing_key = routing_key.split('.')
        base_name = _split_routing_key[3]
        quote_name = _split_routing_key[4]
        exchange_id = data['e']
        symbol = data['s']
        exchange_symbol = f'{symbol}:{exchange_id}'
        if not await self.check_pair_is_active(base_name, exchange_symbol):   # 币种是否绑定了 当前交易对源
            if exchange_symbol in PAIR_PRICE_CACHE:
                PAIR_PRICE_CACHE.pop(exchange_symbol)
            if exchange_symbol in PAIR_PRICE_PASS_CACHE:
                PAIR_PRICE_PASS_CACHE.pop(exchange_symbol)
            return
        if base_name.upper() in S_PASS_PRICE_COIN_LIST: # 判断 币种 是否开启 整数关口预警
            if exchange_symbol not in PAIR_PRICE_PASS_CACHE:
                pair_pass = PricePass(exchange_id, base_name, quote_name)
                pair_pass.pair_data = await get_exchange_pair_data(exchange_id, base_name, quote_name, loop=self._loop)
                PAIR_PRICE_PASS_CACHE[exchange_symbol] = pair_pass
            try:
                await PAIR_PRICE_PASS_CACHE[exchange_symbol].check_price(price=kline[4], tms=kline[0])
            except Exception as e:
                await self.logger.error(e)

        if exchange_symbol not in PAIR_PRICE_CACHE:
            pair_cache = PairCache()
            pair_cache.pair_data = await get_exchange_pair_data(exchange_id, base_name, quote_name, loop=self._loop)
            if base_name.upper() not in S_PRICE_ALARM_COIN_LIST:  # TODO 暂定是一小时 非主流小币种的话 使用另外一套算法
                pair_cache.base_queue = BaseQueue(exchange_id, base_name, quote_name, max_len=settings.SMALL_PERIOD_PRICE_CHECK)
            else:
                pair_cache.base_queue = BaseQueue(exchange_id, base_name, quote_name)
            pair_cache.base_queue.pair_data = pair_cache.pair_data
            PAIR_PRICE_CACHE[exchange_symbol] = pair_cache
        else:
            pair_cache = PAIR_PRICE_CACHE[exchange_symbol]
        if pair_cache.merge_running:
            # 合并计算
            await pair_cache.merge_queue.append(kline)
            if pair_cache.merge_queue.is_expired or pair_cache.merge_queue.is_push:
                # 如果合并计算结束, 则重新进行普通计算
                PAIR_PRICE_CACHE.pop(exchange_symbol)
        else:
            # 普通窗口计算
            await pair_cache.base_queue.append(kline)
            if pair_cache.base_queue.is_push:
                pair_cache.merge_running = True
                insert_base_dates = [x for x in pair_cache.base_queue.queue if x[0] >= pair_cache.base_queue.hit_data[0]]
                if base_name.upper() not in S_PRICE_ALARM_COIN_LIST:    # TODO 小币种 合并期 暂停一小时
                    merge_queue_len = len(insert_base_dates) + settings.SMALL_MERGE_PERIOD_PRICE_CHECK  # 合并计算的分钟数
                    pair_cache.merge_queue = SleepMergeQueue(exchange_id, base_name, quote_name, max_len=merge_queue_len)
                    pair_cache.merge_queue.hit_data = pair_cache.base_queue.hit_data
                else:
                    merge_queue_len = len(insert_base_dates) + settings.MERGE_PERIOD_PRICE_CHECK    # 合并计算的分钟数
                    pair_cache.merge_queue = MergeQueue(exchange_id, base_name, quote_name, max_len=merge_queue_len)
                    pair_cache.merge_queue.queue.extend(insert_base_dates)
                    pair_cache.merge_queue.hit_data = pair_cache.base_queue.hit_data
                    pair_cache.merge_queue.base_is_rise = pair_cache.base_queue.is_rise
                    pair_cache.merge_queue.window_start_tms = pair_cache.base_queue.queue[-1][0]
                    pair_cache.merge_queue.base_pct = pair_cache.base_queue.pct
                    pair_cache.merge_queue.pair_data = pair_cache.pair_data
