#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import time
import json
import zlib
from exspider.utils.http_base import WSBase, logger, HttpBase


class Hotbit(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        now = int(time.time()) // 60 * 60
        self.ws_url = 'wss://ws.hotbit.io'
        self.kline_url = 'https://api.hotbit.io/api/v1/market.kline?market=BTC/USDT&start_time=1558616940&end_time=1558673247&interval=60'
        self.sub_data = {
            "method": "kline.query",
            "params": ["BTCUSDT", now-60, now, 60],
            "id": 100
        }
        self.http = HttpBase(loop=loop, proxy=proxy)

    async def on_message(self, ws, message):
        await self.parse_kline(message, ws)

    async def get_ping_data(self):
        return{
                "method": "server.ping",
                "params": [],
                "id": 100
            }

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            result = zlib.decompress(msg, 16 + zlib.MAX_WBITS)
            data = json.loads(result)
        except Exception as e:
            await logger.error(f'{msg} error: {e}')
            return
        if not data.get('result') or not isinstance(data['result'], list):
            return
        print(data)
        kline_list = data['result']
        tms = 0
        for kline in kline_list:
            # "params": [
            #     [
            #         1492358400,  # 时间戳
            #         "7000.00",  # 开盘价
            #         "8000.0",  # 收盘价
            #         "8100.00",  # 最高价
            #         "6800.00",  # 最低价
            #         "1000.00"  # 成交量
            #         "123456.00",  # 成交金额
            #         "BTCBCC"  # 交易币对市场名称
            #     ]
            # ]
            t = kline[0]
            o = kline[1]
            h = kline[3]
            l = kline[4]
            c = kline[2]
            v = kline[5]
            symbol = kline[7]
            ohlcv = [t, o, h, l, c, v]
            print(f'> {symbol}: {ohlcv}')
            tms = t
        self.sub_data = {
            "method": "kline.query",
            "params": ["BTCUSDT", tms, int(time.time()), 60],
            "id": 100
        }
        await ws.send_json(self.sub_data)


    async def parse_restful_kline(self):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        data = await self.http.get_json_data(self.kline_url)
        if not data or not data.get('result'):
            return []
        ohlcv_list = [[x[0], x[1], x[3], x[4], x[2], x[5]] for x in data['result']]
        return ohlcv_list[-1]


