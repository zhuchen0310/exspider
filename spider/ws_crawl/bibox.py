#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-27 15:57

from __future__ import absolute_import

import base64
import json
import gzip
import time

from spider.ws_crawl import HoldBase


class bibox(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'bibox'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.bibox.com',
            'urls': {
                'symbols': '/v1/mdata?cmd=pairList',
                'trades': '/v1/mdata?cmd=deals',
                'klines': '/v1/mdata?cmd=kline'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://push.bibox.com/'
            }
        }
        self.max_sub_num = 20 # 每个连接 最大订阅数
        self.symbols = self.get_symbols()

    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        return self.ws_data['api']['ws_url']

    async def get_trade_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        now = await self.now_timestamp
        if symbol not in self.symbols:
            return json.dumps({'ping': now})
        return json.dumps(
            {
                "event": "addChannel",
                "channel": "bibox_sub_spot_{}_deals".format(self.symbols[symbol])
            }
        )

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        now = await self.now_timestamp
        if symbol not in self.symbols:
            return json.dumps({'ping': now})
        return json.dumps(
            {
                "event": "addChannel",
                "channel": "bibox_sub_spot_{}_kline_1min".format(self.symbols[symbol])
            }
        )

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades']
        url = f'{api}{path}&pair={self.symbols[symbol]}&size={self.http_data["limits"]["trade"]}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['klines']
        if limit:
            url = f'{api}{path}&pair={self.symbols[symbol]}&period=1min&size={limit}'
        else:
            url = f'{api}{path}&pair={self.symbols[symbol]}&period=1min'
        return url

    async def parse_restful_trade(self, data, symbol, is_save=True):
        """
        功能:
            处理 restful 返回 trade
            封装成统一格式 保存到Redis中
        返回:
            [[1551760709,"10047738192326012742563","ask",3721.94,0.0235]]
        """
        trade_list = []
        if not data:
            return trade_list
        if 'result' not in data or not data['result']:
            return trade_list
        trades_data_list = data['result']
        for x in trades_data_list:
            format_trade = await self.format_trade([
                int(x["time"]) // 1000,  # 秒级时间戳
                x.get("id", f'{int(time.time() * 1000000)}'),
                'buy' if x["side"] in [1, '1'] else 'sell',
                x["price"],
                x["amount"]
            ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        if is_save:
            await self.save_trades_to_redis(symbol, trade_list)
        else:
            return trade_list

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时trade
        """
        try:
            if 'ping' in msg:
                pong = msg.replace('ping', 'pong')
                await ws.send_str(pong)
                return
            data = json.loads(msg)
        except Exception as e:
            return
        if not data or not isinstance(data, list):
            return
        if not data[0].get('data', None):
            return
        en_data = data[0]['data']
        de_data = base64.b64decode(en_data)
        tick_data_list = eval(gzip.decompress(de_data).decode('utf-8'))
        symbol = tick_data_list[0]['pair'].replace('_', '').lower()
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                int(x["time"]) // 1000,  # 秒级时间戳
                x.get("id", f'{int(time.time() * 1000000)}'),
                'buy' if x["side"] in [1, '1'] else 'sell',
                x["price"],
                x["amount"]
            ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        await self.save_trades_to_redis(symbol, trade_list, ws)
        return

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        ohlcv_list = []
        if not data or not data.get('result'):
            return ohlcv_list
        for x in data['result']:
            fmt_k = await self.format_kline([
                x['time'] // 1000,
                x['open'],
                x['high'],
                x['low'],
                x['close'],
                x['vol']
            ])
            if fmt_k:
                ohlcv_list.append(fmt_k)
        return ohlcv_list

    async def parse_kline(self, msg, ws):
        try:
            data = json.loads(msg)
            if "ping" in data:
                pong = msg.replace("ping", "pong")
                await ws.send_str(pong)
                return
            if not isinstance(data, list):
                return
            if len(data) == 0 or "data" not in data[0]:
                return
            en_data = data[0]['data']
            de_data = base64.b64decode(en_data)
            kline_data_list = eval(gzip.decompress(de_data).decode('utf-8'))
            symbol = data[0]['channel'].replace('bibox_sub_spot_', '').replace(
                        '_kline_1min', '').replace('_', '').lower()
        except Exception as e:
            print(e)
            return
        if not kline_data_list:
            return
        kline_data_list = kline_data_list[-2:]
        for kline_data in kline_data_list:
            fmt_k = await self.format_kline([
                int(kline_data["time"]) // 1000,
                kline_data['open'],
                kline_data['high'],
                kline_data['low'],
                kline_data['close'],
                kline_data['vol']
            ])
            await self.save_kline_to_redis(symbol, fmt_k)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data or not data['result']:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x['pair'].replace('_', '').lower():
                x['pair']

            for x in data['result']
        }
        return symbols