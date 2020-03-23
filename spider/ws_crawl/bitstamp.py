#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-29 17:20

from __future__ import absolute_import

import json

from spider.ws_crawl import HoldBase


class bitstamp(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'bitstamp'
        self.http_timeout = 10
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://www.bitstamp.net/api/v2',
            'urls': {
                'symbols': '',
                'trades': '/transactions/{}/',
                'klines': 'https://www.bitstamp.net/ajax/tradeview/price-history/?step=60&currency_pair={}'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://ws.bitstamp.net'
            }
        }
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
                "event": "bts:subscribe",
                "data": {
                    "channel": f'live_trades_{symbol}'
                },
            }
        )

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(symbol)
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 restful 请求的url
        """
        if symbol not in self.symbols:
            return ''
        url = self.http_data['urls']['klines'].format(self.symbols[symbol])
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
        trades_data_list = data
        for x in trades_data_list:
            format_trade = await self.format_trade([
                int(x['date']),
                x["tid"],
                'buy' if int(x["type"]) == 0 else 'sell',
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
            data = json.loads(msg)
        except Exception as e:
            return
        if not data or not data.get('data') or not data.get('channel'):
            return
        symbol = data['channel'].split('_')[-1].lower()
        trade_data = data['data']
        format_trade = await self.format_trade([
            int(trade_data['timestamp']),
            trade_data["id"],
            'buy' if int(trade_data['type']) == 0 else 'sell',
            trade_data['price'],
            trade_data['amount'],
        ])
        if not format_trade:
            trade_list = []
        else:
            trade_list = [format_trade]
        await self.save_trades_to_redis(symbol, trade_list, ws)
        return

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        if not data or not data.get('data'):
            return
        ohlcv_list = []
        for x in data['data']:
            ohlcv = await self.format_kline([
                await self.str_2_timestamp(x['time']),
                float(x['open']),
                float(x['high']),
                float(x['low']),
                float(x['close']),
                float(x['volume']),
            ])
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list

    def get_symbols(self):
        return {
            'btcusd': 'BTC/USD',
            'btceur': 'BTC/EUR',
            'eurusd': 'EUR/USD',
            'xrpusd': 'XRP/USD',
            'xrpeur': 'XRP/EUR',
            'xrpbtc': 'XRP/BTC',
            'ltcusd': 'LTC/USD',
            'ltceur': 'LTC/EUR',
            'ltcbtc': 'LTC/BTC',
            'ethusd': 'ETH/USD',
            'etheur': 'ETH/EUR',
            'ethbtc': 'ETH/BTC',
            'bchusd': 'BCH/USD',
            'bcheur': 'BCH/EUR',
            'bchbtc': 'BCH/BTC',
        }