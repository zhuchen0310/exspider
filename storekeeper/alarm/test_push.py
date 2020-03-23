#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 11:06

"""
内部使用的自定义 价格预警, 邮件push

"""
import ujson
import time

from storekeeper.alarm.base import (
    BaseQueue, PricePass, S_PRICE_ALARM_COIN_LIST, S_PASS_PRICE_COIN_LIST,
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
            pair_cache.base_queue = BaseQueue(exchange_id, base_name, quote_name)
            pair_cache.base_queue.pair_data = pair_cache.pair_data
            PAIR_PRICE_CACHE[exchange_symbol] = pair_cache
        else:
            pair_cache = PAIR_PRICE_CACHE[exchange_symbol]
        # 普通窗口计算
        await pair_cache.base_queue.append(kline)
        if pair_cache.base_queue.is_push:
            await pair_cache.base_queue.reset()
