#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-18 19:13

"""
push center:
    主要用来分发, push 消息, 包括给第三方和通过php push
"""
import asyncio
import datetime
import time
import ujson

from django.conf import settings

from exspider.utils.socket_server import send_notice_to_php
from exspider.utils.pg_con import OHLCVPg
from exspider.utils.redis_con import REDIS_CON
from exspider.utils.logger_con import get_logger
from exspider.utils.enum_con import PushType
from exspider.common import funcs
from storekeeper import platform as platform_model


async_logger = get_logger(__name__, is_debug=settings.IS_DEBUG)
push_logger = get_logger(settings.APP_NAME)

S_PASS_PRICE_COIN_LIST = funcs.get_pass_price_coin()
S_PRICE_ALARM_COIN_LIST = funcs.get_alarm_price_coin()
S_PRICE_PASS_BASE_INTEGER = settings.PRICE_PASS_BASE_INTEGER

# 所有第三方 缓存
PLATFORM_CACHE = {
    x: getattr(platform_model, x)()
    for x in platform_model.platforms
}

# 整数关口缓存
PHP_PASS_PRICE_CACH_MAP = {
    # 'BTC': [13000]
}

async def check_push_2_php(base_name, quote_name, exchange_id) -> bool:
    """
    功能:
        判断 是否 push 到 php 价格异动
    """
    exchange_symbol = f'{base_name}{quote_name}:{exchange_id}'
    r_data = REDIS_CON.hget(settings.COIN_PUSH_FROM_PAIR_KEY, base_name)
    if r_data and r_data.decode() == exchange_symbol:
        return True
    return False


async def check_push_2_platform(platform, base_name, quote_name, exchange_id, push_type: PushType=PushType.rise_or_fall.value) -> bool:
    """
    功能:
        判断 是否 push 到 第三方
    :param base_name:
    :param quote_name:
    :param exchange_id:
    :return:
    """
    # 不属于 监控的交易所
    if exchange_id not in platform.alarm_info:
        return False
    # 价格异动push
    if push_type == PushType.rise_or_fall.value:
        if base_name not in platform.alarm_info[exchange_id]['alarm_coin']:
            return False
        else:
            return True
    # 整数关口
    elif push_type == PushType.pass_price.value:
        if base_name not in platform.alarm_info[exchange_id]['alarm_price_pass']:
            return False
        # 刷新 平台关口记录
        price_pass_cache = platform.alarm_info[exchange_id]['price_pass_cache']
        pass_cache_ex_tms = platform.alarm_info[exchange_id]['pass_cache_ex_tms']
        new_price_pass_cache = {
            x: price_pass_cache[x] for x in price_pass_cache if price_pass_cache[x] >= int(time.time()) - pass_cache_ex_tms
        }
        platform.alarm_info[exchange_id]['price_pass_cache'] = new_price_pass_cache
        return True
    return False


async def push_main(base_name: str, quote_name: str, exchange_id: str, call_php: dict=None, call_platform: dict=None,
              push_type: PushType=''):
    """
    功能:
        push 分发中心
    :param base_name: btc
    :param quote_name: usdt
    :param exchange_id: huobipro
    :param call_php: {'call_func': call_func, 'data': data}
    :param call_platform: {'call_func': call_func, 'data': data}
    :param push_type: 消息类型
    :return:
    """

    if not push_type:
        return
    ret = True
    try:
        if await check_push_2_php(base_name, quote_name, exchange_id):
            is_php_push = True
            if push_type == PushType.pass_price.value:
                coin_symbol = base_name.upper()
                if coin_symbol not in S_PASS_PRICE_COIN_LIST:
                    is_php_push = False
                else:
                    pass_price = call_platform['data']['pass_price']
                    if pass_price % S_PRICE_PASS_BASE_INTEGER:
                        is_php_push = False
                    else:
                        if coin_symbol not in PHP_PASS_PRICE_CACH_MAP:
                            PHP_PASS_PRICE_CACH_MAP[coin_symbol] = [pass_price]
                        else:
                            pass_cache = PHP_PASS_PRICE_CACH_MAP[coin_symbol]
                            if pass_price in pass_cache:
                                is_php_push = False
                            else:
                                PHP_PASS_PRICE_CACH_MAP[coin_symbol] = [pass_price]
            if is_php_push:
                # 判断是否小币种
                if base_name.upper() not in S_PRICE_ALARM_COIN_LIST:  # TODO 小币种测试期间只 push 给限定人员
                    call_php['data'].update({'is_test': 1})
                await async_logger.debug(call_php['data'])
                ret = send_notice_to_php(call_php['call_func'], data=call_php['data'])
                await push_logger.info(call_php['data']['summary'])
        for platform in PLATFORM_CACHE.values():
            if await check_push_2_platform(platform, base_name, quote_name, exchange_id, push_type=push_type):
                ret = await platform.push_message_to_platform(push_type, base_name, exchange_id, call_platform['data'])
    except Exception as e:
        await async_logger.error(e)
        ret = False
    return ret


def push_noon_broadcast():
    """
    功能:
        push 午报
    """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(get_pair_ticker())
    loop.close()


async def get_pair_ticker():
    """
    功能:
        获取一个交易对的ticker 信息, 包括: 当前价格, 今日涨跌幅
    """
    ret_data = {}
    for platform in PLATFORM_CACHE.values():
        for exchange_id in platform.alarm_info:
            redis_key = settings.EXCHANGE_LAST_OHLCV_KEY.format(exchange_id)
            data_list = []
            pg = OHLCVPg(database=exchange_id)
            await pg.init
            today_tms = int(time.mktime(datetime.datetime.now().date().timetuple()))
            noon_broadcast_pairs = platform.alarm_info[exchange_id]['noon_broadcast_pairs']
            for symbol in noon_broadcast_pairs:
                tab_name = f't_{symbol}_1min'
                now = await pg.now_tms
                sql = f'select tms, open from {tab_name} where tms <= {now} and tms >= {today_tms} order by tms limit 1;'
                try:
                    rows = await pg.pool.fetchrow(sql)
                except Exception as e:
                    await async_logger.error(e)
                    continue
                last_ohlcv = REDIS_CON.hget(redis_key, f'{symbol}_m1')
                if last_ohlcv:
                    last_kline = ujson.loads(last_ohlcv)
                    now_price = last_kline[4]
                    today_open_price = float(rows[1])
                    pct = (now_price - today_open_price) / today_open_price * 100
                    is_rise = True if pct > 0 else False
                    pct = round(abs(pct), 2)
                    now_price = now_price if len(f'{now_price}'.split('.')[1]) <= 4 else round(now_price, 4)
                    data_list.append({
                        'pair_name': noon_broadcast_pairs[symbol],
                        'is_rise': is_rise,
                        'pct': pct,
                        'now_price': now_price,
                    })
            await pg.pool.close()
            ret_data[exchange_id] = data_list
            await platform.push_message_to_platform(PushType.noon_broadcast.value, '', exchange_id, data_list)
    return ret_data