#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-22 14:57

import time
import hashlib
import hmac
import base64
import json
from exspider.utils.http_base import WSBase, HttpBase

class biss(WSBase):

    def __init__(self, loop=None, proxy=None, timeout=5):
        super().__init__(loop, proxy, timeout)
        self.APPKEY = 'yMw/v2o4UhYui0fMgNRHgoOcLB0RJBV8j+/SM9tOHcS+dh/Pm+OGNeOASTFxGjOoOlOxgtsIX3ALredZ+adL6YMKER7B3Qe6sQP8MBkpC+PGTxqHKT7YcEmeYS45tjSZ'
        self.APPID = 'ATxp3JdTFgTk21ODVLDk9ohJZkldokhwg4gfO2/yi8P4huDGo+xCBmooWBuhOdCbsH3JWRPHk/VZzTF1R7Ir4TXDoWjuLSBPB1+7/PnBzqRioNfFkO9kiVjk6UPT9Tn6wQ=='
        self.ws_url = 'wss://ws.biki.com/kline-api/ws'
        self.uri = 'https://api.biss.com'
        self.kline_url = '/v1/quote/BTC/USDT/realtime'
        self.trade_url = '/v1/quote/BTC/USDT/tick-history?start=0&count=200'
        self.sub_data = [
            # {"event":"sub","params":{"channel":"market_vdsusdt_kline_1min","cb_id":"vdsusdt"}},
            # {"event":"sub","params":{"channel":"market_eosusdt_kline_1min","cb_id":"eosusdt"}},
            {"event": "sub", "params": {"channel": "market_vdsusdt_trade_ticker", "cb_id": "vdsusdt", "top": 100}}
        ]
        self.http = HttpBase(loop=loop, proxy=proxy)
        self.http.headers = {
            # 'Content-Type': "application/json",
            'x-biss-appid': self.APPID,
            'x-biss-timestamp': '',
            'x-biss-signature': '',

        }
        self.ping_interval_seconds = None

    async def set_headers_before_request(self, url):
        timestamp = f'{int(time.time()) * 1000}'
        message = f'{url}&timestamp={timestamp}'
        signature = base64.b64encode(hmac.new(self.APPKEY.encode(), message.encode(), digestmod=hashlib.sha256).digest()).decode()
        self.http.headers.update({
            'x-biss-timestamp': timestamp,
            'x-biss-signature': signature,
        })

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
            data = json.loads(msg)
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
        if not data or not data.get('tick'):
            return
        tick_data_list = data['tick']['data']
        channel_list = data['channel'].split('_')
        if 'trade' not in channel_list:
            return
        symbol = channel_list[1]
        trade_list = []
        for x in tick_data_list:
            trade_list.append([
                float(x["ts"]) // 1000,
                x["id"],
                'b' if x["side"].lower() == 'bug' else 's',
                x['price'],
                x['vol']
            ])
        print(symbol, trade_list)

    async def parse_restful_trade(self):
        """
        功能:
            处理 restful trade
        :return:
        """
        await self.set_headers_before_request(self.trade_url)
        url = f'{self.uri}{self.trade_url}'
        data = await self.http.get_data(url)
        data = json.loads(data)
        symbol = f"{data['ticker']['symbol']}{data['ticker']['market']}".lower()
        tick_data_list = data['ticks']
        trade_list = []
        for x in tick_data_list:
            trade_list.append([
                float(x["time"]) // 1000,
                x["id"],
                'b' if x["side"].lower() == 'ts_bid' else 's',
                x['price'],
                x['volume']
            ])
        print(symbol, trade_list[0])

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
        await self.set_headers_before_request(self.kline_url)
        url = f'{self.uri}{self.kline_url}'
        print(url, self.http.headers)
        data = await self.http.get_data(url)
        print(data)
        return
        if not data or not data.get('data'):
            return
        ohlcv_list = [
            [
                x[0],
                float(x[1]),
                float(x[2]),
                float(x[3]),
                float(x[4]),
                float(x[5]),
            ]
            for x in data['data']]
        return ohlcv_list[-1]
