#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-16 14:50

import ujson

from prometheus_client import Counter, Enum
from exspider.utils.enum_con import SpiderStatus
from exspider.utils import redis_con as redis
from django.conf import settings

r = redis.REDIS_CON

EXCHANGE_LABELS = ['exchange_id']
EXCHANGE_SPIDER_LABELS = ['exchange_id', 'spider_id']
SYMBOL_LABELS = ['exchange_id', 'symbol']

SPIDER_STATUS = [x.name for x in SpiderStatus]

EXC = 'exchange_count'
EXE = 'exchange_enum'
SPC = 'exchange_spider_count'
SPE = 'exchange_spider_enum'
SYC = 'exchange_spider_symbol_count'
SYE = 'exchange_spider_symbol_enum'


def init(care_type=settings.BASE_WS_TYPE_TRADE):
    EXCHANGE_COUNT = Counter(f'exchange_{care_type}', f'Care Exchange {care_type} Count!')
    EXCHANGE_ENUM = Enum(
        f'exchange_{care_type}_states',
        f'Exchange {care_type} States',
        labelnames=EXCHANGE_LABELS,
        states=SPIDER_STATUS
    )

    EXCHANGE_SPIDER_COUNT = Counter(f'exchange_{care_type}_spider', f'Care Exchange {care_type} Spider Count!')
    EXCHANGE_SPIDER_ENUM = Enum(
        f'exchange_{care_type}_spider_states',
        f'Exchange {care_type} Spider Process States',
        labelnames=EXCHANGE_SPIDER_LABELS,
        states=SPIDER_STATUS
    )

    EXCHANGE_SPIDER_SYMBOL_COUNT = Counter(f'exchange_{care_type}_symbol', f'Care Exchange {care_type} Symbol Count!')
    EXCHANGE_SPIDER_SYMBOL_ENUM = Enum(
        f'exchange_{care_type}_spider_symbol_states',
        f'Exchange {care_type} Spider Symbols States',
        labelnames=SYMBOL_LABELS,
        states=SPIDER_STATUS
    )

    return {
        EXC: EXCHANGE_COUNT,
        EXE: EXCHANGE_ENUM,
        SPC: EXCHANGE_SPIDER_COUNT,
        SPE: EXCHANGE_SPIDER_ENUM,
        SYC: EXCHANGE_SPIDER_SYMBOL_COUNT,
        SYE: EXCHANGE_SPIDER_SYMBOL_ENUM,
    }

# 初始化
CARE_TYPE_MAP = {
    settings.BASE_WS_TYPE_TRADE: init(settings.BASE_WS_TYPE_TRADE),
    settings.BASE_WS_TYPE_KLINE: init(settings.BASE_WS_TYPE_KLINE)
}


def exchange_status():
    for care_type in CARE_TYPE_MAP:
        exchange_data = r.hgetall(redis.S_EXCHANGE_STATUS_KEY_MAP[care_type])
        if not exchange_data:
            continue
        data_list = [(x.decode(), int(exchange_data[x])) for x in exchange_data]
        for exchange_id, states in data_list:
            CARE_TYPE_MAP[care_type][EXE].labels(exchange_id).state(SpiderStatus(states).name)
        CARE_TYPE_MAP[care_type][EXC].inc()


def exchange_spider_status():
    for care_type in CARE_TYPE_MAP:
        exchange_data = r.hkeys(redis.S_EXCHANGE_STATUS_KEY_MAP[care_type])
        if not exchange_data:
            continue
        exchange_ids = [x.decode() for x in exchange_data]
        for exchange_id in exchange_ids:
            ex_spider_key = redis.S_SPIDER_STATUS_KEY_MAP[care_type].format(exchange_id)
            spider_data = r.hgetall(ex_spider_key)
            if not spider_data:
                continue
            spider_dict = {x.decode(): ujson.loads(spider_data[x]) for x in spider_data}
            for spider_id in spider_dict:
                states = spider_dict[spider_id][redis.STATUS_KEY]
                CARE_TYPE_MAP[care_type][SPE].labels(exchange_id, spider_id).state(SpiderStatus(states).name)
        CARE_TYPE_MAP[care_type][SPC].inc()

def exchange_spider_symbol_status():
    for care_type in CARE_TYPE_MAP:
        exchange_data = r.hkeys(redis.S_EXCHANGE_STATUS_KEY_MAP[care_type])
        if not exchange_data:
            continue
        exchange_ids = [x.decode() for x in exchange_data]
        for exchange_id in exchange_ids:
            ex_key = redis.S_SYMBOL_STATUS_KEY_MAP[care_type].format(exchange_id)
            s_data = r.hgetall(ex_key)
            symbols = {x.decode(): ujson.loads(s_data[x]) for x in s_data}
            for symbol in symbols:
                states = symbols[symbol][redis.STATUS_KEY]
                CARE_TYPE_MAP[care_type][SYE].labels(exchange_id, symbol).state(SpiderStatus(states).name)
        CARE_TYPE_MAP[care_type][SYC].inc()

