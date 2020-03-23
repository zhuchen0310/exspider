#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import json
from exspider.utils.http_base import WSBase, logger, HttpBase


class gateio(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.ws_url = 'wss://ws.gate.io/v3/'
        self.kline_url = 'https://data.gateio.co/api2/1/candlestick2/btc_usdt?group_sec=60&range_hour=4'
        self.sub_data = [
            {
                "id": 23232,
                "method": "kline.subscribe",
                "params": ["BTC_USDT", 60]
            },
            {
                "id": 23232,
                "method": "kline.subscribe",
                "params": ["ETH_USDT", 60]
            },
        ]
        self.http = HttpBase(loop=loop, proxy=proxy)

    async def on_message(self, ws, message):
        await self.parse_kline(message, ws)
        # await logger.info(message)

    async def get_ping_data(self):
        return json.dumps({"id":1551397, "method":"server.ping", "params":[]})

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            data = json.loads(msg)
        except Exception as e:
            return
        if not data.get('params'):
            return
        kline_list = data['params']
        if not kline_list or not isinstance(kline_list, list):
            return
        symbol = kline_list[0][-1].lower().replace('_', '')
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

    async def parse_restful_kline(self):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        data = await self.http.get_json_data(self.kline_url)
        if not data or not data.get('data'):
            return []
        ohlcv_list = [[int(x[0]) // 1000, x[5], x[3], x[4], x[2], x[1]] for x in data['data']]
        return ohlcv_list[-1]

