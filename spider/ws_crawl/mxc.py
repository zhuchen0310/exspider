#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-24 14:18

import ujson

from django.conf import settings
from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE


class mxc(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'mxc'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://www.mxc.com',
            'urls': {
                'symbols': '/open/api/v1/data/markets_info',
                'trades': '/open/api/v1/data/history?market={}',
                'klines': '/open/api/v1/data/kline?market={}&interval=1m&startTime={}'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://www.mxc.com/socket.io/?EIO=3&transport=websocket'
            }
        }
        self.symbols = self.get_symbols()
        self.max_sub_num = 1
        self.ping_interval_seconds = 5

    async def get_ping_data(self):
        return '2'

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
        return f'42{ujson.dumps(["sub.symbol",{"symbol": self.symbols[symbol]}])}'

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        return f'42{ujson.dumps(["sub.kline",{"symbol": self.symbols[symbol],"interval":"Min1"}])}'

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(self.symbols[symbol])
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 restful 请求的url
        """
        now = await self.now_timestamp
        start_tms = now - settings.CHECK_KLINES_TMS
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(self.symbols[symbol], start_tms)
        if limit:
            url = f'{api}{path}&limit={limit}'
        else:
            url = f'{api}{path}'
        return url

    async def parse_restful_trade(self, data, symbol, is_save=True):
        """
        功能:
            处理 restful 返回 trade
            封装成统一格式 保存到Redis中
        返回:
            [[1551760709,"10047738192326012742563","ask",3721.94,0.0235]]
            trade_id = '1-1551760709-3721.94-0.0235'
        """
        trade_list = []
        if not data:
            return trade_list
        if 'data' not in data or not data['data']:
            return trade_list
        trades_data_list = data['data']
        for x in trades_data_list:
            t = await self.str_2_timestamp(x['tradeTime'], is_timedelta=False)
            t_id = t // 60 * 60
            format_trade = await self.format_trade([
                t,
                f'{x["tradeType"]}-{t_id}-{x["tradePrice"]}-{x["tradeQuantity"]}',
                'b' if x['tradeType'] in [1, '1'] else 's',
                x["tradePrice"],
                x["tradeQuantity"],
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
            if not msg or 'push.symbol' not in msg or 'deals' not in msg:
                return
            data = ujson.loads(msg.replace('42', ''))
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
            t = x['t'] // 1000
            t_id = t // 60 * 60
            format_trade = await self.format_trade([
                t,
                f'{x["T"]}-{t_id}-{x["p"]}-{x["q"]}',
                'b' if x['T'] == 1 else 's',
                x['p'],
                x['q']
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
        if not data or not data.get('data', None):
            return ohlcv_list
        for x in data['data']:
            fmt_k = await self.format_kline([
                x[0],
                x[1],
                x[3],
                x[4],
                x[2],
                x[5],
            ])
            if fmt_k:
                ohlcv_list.append(fmt_k)
        return ohlcv_list

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            if not msg or 'push.kline' not in msg:
                return
            data = ujson.loads(msg.replace('42', ''))
        except Exception as e:
            data = None
        if not data:
            return
        kline = data[1]['data']
        symbol = kline['symbol'].replace('_', '').lower()
        ohlcv = await self.format_kline([
            kline['t'],
            kline['o'],
            kline['h'],
            kline['l'],
            kline['c'],
            kline['q'],
        ])
        await self.save_kline_to_redis(symbol, ohlcv)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data or not data.get('data'):
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x.replace('_', '').lower(): x
            for x in data['data']
        }
        return symbols
