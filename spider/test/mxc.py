#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-22 14:57

import json
from exspider.utils.http_base import WSBase, HttpBase

class mxc(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.ws_url = 'wss://www.mxc.com/socket.io/?EIO=3&transport=websocket'
        self.kline_url = 'https://www.mxc.com/open/api/v1/data/kline?market=BTC_USDT&interval=1m&startTime={}'
        self.sub_data = [
            '42["sub.kline",{"symbol":"BTC_USDT","interval":"Min1"}]',
            '42["sub.symbol",{"symbol":"BTC_USDT"}]',
        ]
        self.http = HttpBase(loop=loop, proxy=proxy)
        self.ping_interval_seconds = 5

    async def on_message(self, ws, message):
        await self.parse_kline(message, ws)
        await self.parse_trade(message, ws)

    async def get_ping_data(self):
        return '2'

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            if not msg or 'push.kline' not in msg:
                return
            data = json.loads(msg.replace('42', ''))
        except Exception as e:
            data = None
        if not data:
            return
        kline = data[1]['data']
        symbol = kline['symbol'].replace('_', '').lower()
        ohlcv = [
            kline['t'],
            float(kline['o']),
            float(kline['h']),
            float(kline['l']),
            float(kline['c']),
            float(kline['q']),
        ]
        print(symbol, ohlcv)

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时 trade
        """
        try:
            if not msg or 'push.symbol' not in msg or 'deals' not in msg:
                return
            data = json.loads(msg.replace('42', ''))
            ticker_data = data[1]
            if not ticker_data or not ticker_data.get('data'):
                return
        except Exception as e:
            print(e)
            return
        tick_data_list = ticker_data['data']['deals']
        symbol = ticker_data['symbol'].replace('_', '').lower()
        trade_list = []
        for x in tick_data_list:
            trade_list.append([
                x['t'] // 1000,
                x['t'],
                'b' if x['T'] == 1 else 's',
                x['p'],
                x['q']
            ])
        print(symbol, trade_list)

    async def parse_restful_kline(self):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        now = await self.now_timestamp
        data = await self.http.get_json_data(self.kline_url.format(now-60*60))
        if not data or not data.get('data'):
            return
        ohlcv_list = [
            [
                x[0],
                float(x[1]),
                float(x[3]),
                float(x[4]),
                float(x[2]),
                float(x[5]),
            ]
            for x in data['data']]
        return ohlcv_list[-1]



if __name__ == '__main__':
    import asyncio
    ex = mxc()
    # asyncio.get_event_loop().run_until_complete(ex.parse_restful_kline())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ex.add_sub_data(ex.sub_data))
    loop.run_until_complete(ex.get_ws_data_forever(ex.ws_url))