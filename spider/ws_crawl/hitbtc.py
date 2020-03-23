#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-29 15:44
from __future__ import absolute_import

import json

from spider.ws_crawl import HoldBase


class hitbtc(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'hitbtc'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.hitbtc.com/api/2/public',
            'urls': {
                'symbols': '/symbol',
                'trades': '/trades/{}',
                'klines': '/candles/{}?period=M1&limit={}'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://api.hitbtc.com/api/2/ws'
            }
        }
        self.symbols = self.get_symbols()
        self.is_check_first_restful = False # 首次不check restful API
        self.max_sub_num = 10

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
                "method": "subscribeTrades",
                "params": {
                    "symbol": symbol.upper(),
                    "limit": self.http_data["limits"]["trade"]
                  },
                "id": await self.now_timestamp
            }
        )

    async def get_kline_sub_data(self, symbol):
        now = await self.now_timestamp
        if symbol not in self.symbols:
            return json.dumps({'ping': now})
        return json.dumps(
            {
                "method": "subscribeCandles",
                "params": {
                    "symbol": symbol.upper(),
                    "period": "M1",
                    "limit": 1
                },
                "id": await self.now_timestamp
            }
        )

    async def get_ping_data(self):
        return json.dumps({'ping': await self.now_timestamp})

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(symbol.upper())
        url = f'{api}{path}?limit={self.http_data["limits"]["trade"]}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(symbol.upper(), self.http_data["limits"]["kline"])
        url = f'{api}{path}'
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
                await self.str_2_timestamp(x['timestamp']),
                x["id"],
                x["side"],
                x["price"],
                x["quantity"],
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
        if not data or not data.get('params'):
            return
        params_data = data.get('params')
        symbol = params_data['symbol'].lower()
        trade_data_list = params_data['data']
        trade_list = []
        for trade_data in trade_data_list:
            format_trade = await self.format_trade([
                await self.str_2_timestamp(trade_data['timestamp']),
                trade_data["id"],
                trade_data["side"],
                trade_data["price"],
                trade_data["quantity"],
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
        if not data:
            return
        ohlcv_list = []
        for x in data:
            ohlcv = await self.format_kline(
                [
                    await self.str_2_timestamp(x['timestamp']),
                    float(x['open']),
                    float(x['max']),
                    float(x['min']),
                    float(x['close']),
                    float(x['volume']),
                ]
            )
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list

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
            await self.logger.error(e)
            data = None
        if not data:
            return
        kline_list = data['data']
        symbol = data['symbol'].lower()
        for kline in kline_list:
            ohlcv = await self.format_kline([
                await self.str_2_timestamp(kline['timestamp']),
                float(kline['open']),
                float(kline['max']),
                float(kline['min']),
                float(kline['close']),
                float(kline['volume']),
            ])
            await self.save_kline_to_redis(symbol, ohlcv)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x['id'].lower():
                f"{x['baseCurrency']}/{x['quoteCurrency']}".upper()

            for x in data
        }
        return symbols