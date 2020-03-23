#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51
import json
from exspider.utils.http_base import WSBase, logger, HttpBase


class hitbtc(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.ws_url = 'wss://api.hitbtc.com/api/2/ws'
        self.kline_url = 'https://api.hitbtc.com/api/2/public/candles/BTCUSD?period=M1'
        self.sub_data = {
            "method": "subscribeCandles",
            "params": {
                "symbol": 'ETHBTC',
                "period": "M1",
                "limit": 1
            },
            "id": 1212121
        }
        self.http = HttpBase(loop=loop, proxy=proxy)

    async def on_message(self, ws, message):
        await self.parse_kline(message, ws)

    async def get_ping_data(self):
        return '{"ping": 1232323}'

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            ret = json.loads(msg)
            if 'error' in ret and ret['error']['code'] in [2001, '2001']:
                return
            if 'params' not in ret:
                return
            data = ret['params']
        except Exception as e:
            data = None
        if not data:
            return
        kline_list = data['data']
        symbol = data['symbol'].lower()
        for kline in kline_list:
            ohlcv = [
                kline['timestamp'],
                float(kline['open']),
                float(kline['max']),
                float(kline['min']),
                float(kline['close']),
                float(kline['volume']),
            ]
            print(symbol, ohlcv)

    async def parse_restful_kline(self):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        data = await self.http.get_json_data(self.kline_url)
        if not data:
            return
        ohlcv_list = [
            [
                x['timestamp'],
                float(x['open']),
                float(x['max']),
                float(x['min']),
                float(x['close']),
                float(x['volume']),
            ]
            for x in data]
        return ohlcv_list[-1]
