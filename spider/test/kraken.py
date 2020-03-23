#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import json
import time
from exspider.utils.http_base import WSBase, logger, HttpBase


class kraken(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.ws_url = 'wss://ws.kraken.com'
        self.kline_url = 'https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=1'
        self.sub_data = {
              "event": "subscribe",
              "pair": [
                "XBT/EUR", 'XBT/USD'
              ],
              "subscription": {
                "name": "ohlc",
                "interval": 1
              }
            }
        self.http = HttpBase(loop=loop, proxy=proxy)

    async def on_message(self, ws, message):
        await self.parse_kline(message, ws)

    async def get_ping_data(self):
        return{
                "reqid": int(time.time()),
                "event":"ping"
            }

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            data = json.loads(msg)
        except Exception as e:
            await logger.error(f'{msg} error: {e}')
            return
        if not data or not isinstance(data, list):
            return
        kline = data[1]
        symbol = data[0]
        t = float(kline[0]) // 60 * 60
        o = kline[2]
        h = kline[3]
        l = kline[4]
        c = kline[5]
        v = kline[7]
        ohlcv = [t, o, h, l, c, v]
        print(f'> {symbol}: {ohlcv}')

    async def parse_restful_kline(self):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        data = await self.http.get_json_data(self.kline_url)
        if not data or not data.get('result'):
            return []
        ohlcv_list = [[x[0], x[1], x[2], x[3], x[4], x[6]] for x in list(data['result'].values())[0]]
        return ohlcv_list[-1]
