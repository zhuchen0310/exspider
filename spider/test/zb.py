#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-22 14:57

import gzip
import json
from exspider.utils.http_base import WSBase, HttpBase

class zb(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.ws_url = 'wss://api.zb.cn/websocket'
        self.kline_url = 'http://api.zb.cn/data/v1/kline?market=btc_usdt'
        self.sub_data = [
            {'event': 'addChannel', 'channel': 'btcusdt_trades'},
            {'event': 'addChannel', 'channel': 'btcusdt_depth'}
        ]
        self.http = HttpBase(loop=loop, proxy=proxy)
        self.http.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36'
        }
        self.ping_interval_seconds = None

    async def on_message(self, ws, message):
        # print(message)
        # result = gzip.decompress(message).decode()
        # print(result)
        # await self.parse_kline(message, ws)
        await self.parse_trade(message, ws)

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        原始数据:
            {
                "event_rep":"",
                "channel":"market_vdsusdt_kline_1min",
                "data":null,
                "tick":{
                    "amount":1868.736778,
                    "close":3.4601,
                    "ds":"2019-06-28 11:31:00",
                    "high":3.4699,
                    "id":1561692660,
                    "low":3.4601,
                    "open":3.4699,
                    "tradeId":824102,
                    "vol":539.7},
                "ts":1561692682000,"status":"ok"}

        """

        try:
            result = gzip.decompress(msg).decode()
            if 'ping' in result:
                await self.send(ws, result.replace('ping', 'pong'))
            data = json.loads(result)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('tick'):
            return
        kline = data['tick']
        channel_list = data['channel'].split('_')
        if 'kline' not in channel_list:
            return
        symbol = channel_list[1]
        ohlcv = [
            int(kline["id"]),
            float(kline["open"]),
            float(kline["high"]),
            float(kline["low"]),
            float(kline["close"]),
            float(kline["vol"]),
        ]
        print(symbol, ohlcv)

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时 trade
        原始数据:
            {
                'event_rep': '',
                'channel': 'market_vdsusdt_trade_ticker',
                'data': None,
                'tick': {
                    'data': [
                        {
                            'amount': '166.679526',
                            'ds': '2019-06-28 11:55:05',
                            'id': 824233,
                            'price': '3.4374',
                            'side': 'SELL',
                            'ts': 1561694105000,
                            'vol': '48.49'
                        }],
                    'ts': 1561694105000
                    },
                'ts': 1561694105000,
                'status': 'ok'
            }

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
        print(msg)
        symbol = channel_list[0]
        trade_list = []
        for x in tick_data_list:
            trade_list.append([
                int(x["date"]),
                x["tid"],
                'b' if x["type"].lower() == 'buy' else 's',
                x['price'],
                x['amount']
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
        if not data or not data.get('data'):
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
            for x in data['data']]
        return ohlcv_list[-1]
