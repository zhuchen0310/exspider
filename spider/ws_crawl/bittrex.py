#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-28 14:16

from __future__ import absolute_import

import json
from base64 import b64decode
from zlib import decompress, MAX_WBITS
from urllib.parse import urlencode

import requests

from spider.ws_crawl import HoldBase


class bittrex(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'bittrex'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.bittrex.com/api/v1.1/public',
            'urls': {
                'symbols': '/getmarkets',
                'trades': '/getmarkethistory?market={}',
                'klines': 'https://international.bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}&tickInterval=oneMin'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://socket.bittrex.com/signalr/connect',
                'data_api': 'https://socket.bittrex.com/signalr/negotiate'
            }

        }
        self.symbols = self.get_symbols()


    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data or not data.get('result'):
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            f'{x["MarketCurrency"]}{x["BaseCurrency"]}'.lower():
                x['MarketName']
            for x in data['result']
        }
        return symbols

    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        connectionData = '[{"name":"c2"}]'
        clientProtocol = '1.5'
        transport = 'webSockets'
        tid = '1'
        try:
            if self.http_proxy:
                proxies = {
                    'http': self.http_proxy, 'https': self.http_proxy
                }
            else:
                proxies = None
            data = requests.get(self.ws_data['api']['data_api'],
                                params={"clientProtocol": clientProtocol, "connectionData": connectionData},
                                proxies=proxies).json()
        except Exception as e:
            raise BaseException(f'{self.exchange_id} > GET Token Error: {e}')
        connectionToken = data['ConnectionToken']

        req_data = {
            'connectionToken': connectionToken,
            'connectionData': connectionData,
            'tid': tid,
            'transport': transport,
            'clientProtocol': clientProtocol,
        }
        params = urlencode(req_data)
        url = f'{self.ws_data["api"]["ws_url"]}?{params}'
        return url

    async def get_trade_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        data = {
            'H': 'c2',
            'M': 'QueryExchangeState',
            'A': (),
            'I': 0
        }
        if symbol in self.symbols:
            A = [self.symbols[symbol]]
            data.update({'A': A})
        return json.dumps(data)

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(self.symbols.get(symbol, ''))
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        url = self.http_data['urls']['klines'].format(self.symbols.get(symbol, ''))
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
        if not data or not data.get('result'):
            return trade_list
        trades_data_list = data['result']
        for x in trades_data_list:
            format_trade = await self.format_trade([
                await self.str_2_timestamp(x['TimeStamp']),  # 秒级时间戳
                x["Id"],
                x["OrderType"].lower(),
                x["Price"],
                x["Quantity"]
            ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        if is_save:
            await self.save_trades_to_redis(symbol, trade_list)
        else:
            return trade_list

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        if not data or not data.get('result'):
            return
        ohlcv_list = []
        for x in data['result']:
            ohlcv = await self.format_kline(
                [
                    await self.str_2_timestamp(x['T']),
                    float(x['O']),
                    float(x['H']),
                    float(x['L']),
                    float(x['C']),
                    float(x['V']),
                ]
            )
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时trade
        """
        try:
            data = json.loads(msg)
            if not data.get('R'):
                return
            en_data = data['R']
            try:
                deflated_msg = decompress(
                    b64decode(en_data, validate=True), -MAX_WBITS)
            except SyntaxError:
                deflated_msg = decompress(b64decode(en_data, validate=True))
            data = json.loads(deflated_msg)
        except Exception as e:
            return
        if 'M' not in data or 'f' not in data:
            return
        symbol = ''.join(data['M'].split('-')[::-1]).lower()
        tick_data_list = data['f']
        trade_list = []
        for tick_data in tick_data_list:
            format_trade = await self.format_trade([
                int(tick_data['T']) // 1000,  # 秒
                tick_data["I"],
                tick_data['OT'].lower(),
                tick_data['P'],
                tick_data['Q'],
            ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        await self.save_trades_to_redis(symbol, trade_list, ws)
        # TODO 这个交易所 特殊 只能循环订阅
        query_data = await self.get_trade_sub_data(symbol)
        await ws.send_str(query_data)
        return