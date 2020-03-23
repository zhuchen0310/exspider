#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 16:36

from __future__ import absolute_import

import json
import gzip

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE
from exspider.utils.enum_con import TimeFrame


class huobipro(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'huobipro'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept-Language': 'zh-cn',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.huobi.pro',
            'urls': {
                'symbols': '/v1/common/symbols',
                'trades': '/market/history/trade',
                'klines': '/market/history/kline'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://api.huobi.pro/ws'
            }
        }
        self.symbols = self.get_symbols()
        self.time_frame = {
            TimeFrame.m1.value: '1min',
            TimeFrame.m5.value: '5min',
            TimeFrame.m15.value: '15min',
            TimeFrame.m30.value: '30min',
            TimeFrame.h1.value: '60min',
            TimeFrame.d1.value: '1day',
            TimeFrame.M1.value: '1mon',
            TimeFrame.w1.value: '1week',
            TimeFrame.Y1.value: '1year',
        }
        self.max_sub_num = 50

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
        return json.dumps(
            {
                "sub": f"market.{symbol}.trade.detail",
                "id": f"{now}"
            }
        )

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        now = await self.now_timestamp
        return json.dumps(
            {
                "sub": f"market.{symbol}.kline.1min",
                "id": f"{now}"
            }
        )

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades']
        url = f'{api}{path}?symbol={symbol}&size={self.http_data["limits"]["trade"]}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 restful 请求的url
        """
        if timeframe not in self.time_frame:
            return '1min'
        else:
            timeframe = self.time_frame[timeframe]
        api = self.http_data['api']
        path = self.http_data['urls']['klines']
        if limit:
            url = f'{api}{path}?symbol={symbol}&size={limit}&period={timeframe}'
        else:
            url = f'{api}{path}?symbol={symbol}&period={timeframe}'
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
        if 'data' not in data or not data['data']:
            return trade_list
        if 'ch' not in data:
            return trade_list
        trades_data_list = data['data']
        for trades_data in trades_data_list:
            for x in trades_data.get('data', []):
                format_trade = await self.format_trade([
                    x["ts"] // 1000,  # 秒级时间戳
                    x["id"],
                    x["direction"],
                    x["price"],
                    x["amount"],
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
            result = gzip.decompress(msg).decode()
            if 'ping' in result:
                pong = result.replace('ping', 'pong')
                await ws.send_str(pong)
                return
            data = json.loads(result)
        except Exception as e:
            print(e)
            return
        if 'tick' not in data or 'data' not in data['tick']:
            if data.get('status', '') == 'error':
                await self.logger.error(f'{data}')
            return
        tick = data['tick']
        tick_data_list = tick['data']
        symbol = data['ch'].split('.')[1].lower()
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                x["ts"] // 1000,  # 秒级时间戳
                x["id"],
                x["direction"],
                x["price"],
                x["amount"],
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
                x['id'],
                x['open'],
                x['high'],
                x['low'],
                x['close'],
                x['amount']
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
            result = gzip.decompress(msg).decode()
            if 'ping' in result:
                pong = result.replace('ping', 'pong')
                await ws.send_str(pong)
                return
            data = json.loads(result)
        except Exception as e:
            await self.logger.error(f'{msg} error: {e}')
            return

        if not data or not data.get('tick', None):
            if data.get('status', '') == 'error':
                await self.logger.error(f'{data}')
            return
        kline = data['tick']
        ts_id = kline['id']
        timestamp = int(ts_id)
        symbol = data['ch'].split('.')[1].lower()
        ohlcv = await self.format_kline([
            timestamp,
            kline['open'],
            kline['high'],
            kline['low'],
            kline['close'],
            kline['amount'],
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
            x['symbol'].lower():
                f"{x['base-currency']}/{x['quote-currency']}".upper()

            for x in data['data']
        }
        return symbols
