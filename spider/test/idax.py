#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-22 14:57

import json
from exspider.utils.http_base import WSBase, HttpBase

class idax(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.headers = {'Content-Type': "application/json"}
        self.ws_url = 'wss://openws.idax.pro/ws'
        self.kline_url = 'https://openapi.idax.pro/api/v2/kline?pair=BTC_USDT&period=1min&size=200'
        self.sub_data = [
            {'event':'addChannel','channel':'idax_sub_eth_btc_kline_1min'},
            {'event':'addChannel','channel':'idax_sub_eth_usdt_kline_1min'},
            {'event': 'addChannel', 'channel': 'idax_sub_eth_btc_trades'}
        ]
        self.http = HttpBase(loop=loop, proxy=proxy)
        self.ping_interval_seconds = 5

    async def on_message(self, ws, message):
        await self.parse_kline(message, ws)
        # print(message)
        # await self.parse_trade(message, ws)

    async def get_ping_data(self):
        return json.dumps({"event":"ping"})

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        原始数据:
            {
                "channel":"idax_sub_eth_btc_kline_1min",
                "code":"00000",
                "data":["1561622640000","0.026052","0.026053","0.026046","0.026048","1.26427602"] # t, o, h, l, c, v
            }
        """

        try:
            data = json.loads(msg)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('data'):
            return
        kline = data['data']
        channel_list = data['channel'].split('_')
        if 'kline' not in channel_list:
            return
        symbol = f'{channel_list[2]}{channel_list[3]}'
        ohlcv = [
            int(kline[0]) // 1000,
            float(kline[1]),
            float(kline[2]),
            float(kline[3]),
            float(kline[4]),
            float(kline[5]) / float(kline[4]),
        ]
        print(symbol, ohlcv)

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时 trade
        原始数据:
            {
                "channel":"idax_sub_eth_btc_trades",
                "code":"00000",
                "data":[[2602708510000030972,"0.026225","4.673",1561625122754,"sell"],[2602708510000030973,"0.026218","4.447",1561625122754,"sell"]]}
        [Transaction number, price, volume, Clinch a deal the time, Clinch a deal the type（buy|sell）]
        """
        try:
            data = json.loads(msg)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('data'):
            return
        tick_data_list = data['data']
        channel_list = data['channel'].split('_')
        if 'trades' not in channel_list:
            return
        symbol = f'{channel_list[2]}{channel_list[3]}'
        trade_list = []
        for x in tick_data_list:
            trade_list.append([
                float(x[3]) // 1000,
                x[0],
                'b' if x[4] == 'buy' else 's',
                x[1],
                x[2]
            ])
        print(symbol, trade_list)

    async def parse_restful_kline(self):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]

        交易所数据:
                [
            1417449600000,		时间戳
            "2370.16",			开
            "2380",				高
            "2352",				低
            "2367.37",			收
            "17259.83",			交易量
        ]
        """
        data = await self.http.get_json_data(self.kline_url)
        if not data or not data.get('kline'):
            return
        ohlcv_list = [
            [
                x[0] // 1000,
                float(x[1]),
                float(x[2]),
                float(x[3]),
                float(x[4]),
                float(x[5]),
            ]
            for x in data['kline']]
        return ohlcv_list[-1]



if __name__ == '__main__':
    import asyncio
    ex = mxc()
    # asyncio.get_event_loop().run_until_complete(ex.parse_restful_kline())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ex.add_sub_data(ex.sub_data))
    loop.run_until_complete(ex.get_ws_data_forever(ex.ws_url))