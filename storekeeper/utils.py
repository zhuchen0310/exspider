#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:16

import sys

from django.conf import settings

from exspider.utils.pg_con import OHLCVPg
from exspider.utils.enum_con import TimeFrame


async def handle_csv_line(line):
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
    ohlcv = line.split(',')
    return [
        int(ohlcv[0]),
        float(ohlcv[1]),
        float(ohlcv[2]),
        float(ohlcv[3]),
        float(ohlcv[4]),
        float(ohlcv[5]),
    ]

async def go_insert_ohlcv(exchange_id, symbol):
    """
    功能:
        读取 管道中的trade 数据 保存到 pg中
    """
    table_name = f'{symbol}_{TimeFrame.m1.value}'
    if not (exchange_id and symbol):
        print('传参错误!')
        return
    ohlcv_list = []
    for line in sys.stdin:
        ohlcv = await handle_csv_line(line)
        if not ohlcv:
            continue
        ohlcv_list.append(ohlcv)
        if len(ohlcv_list) >= settings.ONE_TIME_SAVE_COUNT:
            new_ohlcv_list = ohlcv_list
            trade_list = []
            try:
                async with OHLCVPg(exchange_id) as pg:
                    await pg.insert_many(table_name, new_ohlcv_list)
            except:
                trade_list.extend(new_ohlcv_list)
    else:
        try:
            async with OHLCVPg(exchange_id) as pg:
                await pg.insert_many(table_name, ohlcv_list)
        except Exception as e:
            print(e)



