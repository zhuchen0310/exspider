#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-18 16:26

from __future__ import absolute_import

import os
import json
import datetime

from spider.ws_crawl import HoldBase, WS_TYPE_KLINE, WS_TYPE_TRADE


class bitmex(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'bitmex'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://www.bitmex.com/api/v1',
            'urls': {
                'symbols': '/instrument/activeAndIndices',
                'trades': '/trade',
                'klines': '/trade/bucketed'
            },
            'limits': {
                'kline': 750,
                'trade': 500,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://www.bitmex.com/realtime'
            }
        }
        self.has_aicoin_symbol_map = True
        self.reverse_symbols = None
        self.symbols = self.get_symbols()
        self.max_sub_num = 3
        self.ping_interval_seconds = 5

    def get_symbols(self):
        """
        功能:
            初始化 交易所 的 所有交易对
        """
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {}
        reverse_symbols = {}
        for x in data:
            if x['state'] != 'Open':
                continue
            ex_symbol = x['symbol'].lower()
            symbol = ex_symbol
            if symbol == 'xbtusd':
                symbol = 'btcusd'
            symbols[symbol] = ex_symbol
            reverse_symbols[ex_symbol] = symbol
        self.reverse_symbols = reverse_symbols
        return symbols

    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        return self.ws_data['api']['ws_url']

    async def get_trade_sub_data(self, symbols):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        return json.dumps({
            "op": "subscribe",
            "args": [f"trade:{self.symbols[symbol].upper()}" for symbol in symbols]
        })

    async def get_ping_data(self):
        """
        功能:
            获取 ping
        """
        return 'ping'

    async def get_kline_sub_data(self, symbols):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        return json.dumps({
            "op": "subscribe",
            "args": [f"tradeBin1m:{self.symbols[symbol].upper()}" for symbol in symbols]
        })

    async def send_the_first_sub(self, send_sub_datas, ws, ws_type=None, pending_symbols=None):
        """
        功能:
            建立ws 连接后 发送订阅消息
            首次 获取任意待启动的
            重连 只获取当前脚本的
        """
        if not self.is_send_sub_data:
            return
        send_sub_datas = [] if not send_sub_datas else send_sub_datas
        if pending_symbols:
            if ws_type == WS_TYPE_TRADE:
                send_sub_datas.append(
                    await self.get_trade_sub_data(pending_symbols)
                )
            elif ws_type == WS_TYPE_KLINE:
                send_sub_datas.append(
                    await self.get_kline_sub_data(pending_symbols)
                )
            send_sub_datas = set(send_sub_datas)
        for sub_data in send_sub_datas:
            await ws.send_str(sub_data)

    async def send_new_symbol_sub(self, pending_symbols, ws, ws_type=None):
        """
        功能:
            重连 以后 检测是否有新的订阅
            binance 这种的通过url订阅的, 只能先关闭, 再重新启动
        """
        new_send_sub_datas = []
        if ws_type == WS_TYPE_TRADE:
            new_send_sub_datas = [
                await self.get_trade_sub_data(pending_symbols)
            ] if pending_symbols else []
        elif ws_type == WS_TYPE_KLINE:
            new_send_sub_datas = [
                await self.get_kline_sub_data(pending_symbols)
            ] if pending_symbols else []
        for sub_data in new_send_sub_datas:
            await ws.send_str(sub_data)

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades']
        url = f'{api}{path}?symbol={self.symbols[symbol].upper()}&count={self.http_data["limits"]["trade"]}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['klines']
        url = f'{api}{path}?binSize=1m&partial=true&symbol={self.symbols[symbol].upper()}&count={self.http_data["limits"]["kline"]}&reverse=true'
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
        for trades_data in data:
            format_trade = await self.format_trade([
                await self.str_2_timestamp(trades_data["timestamp"], timedelta_hours=8),
                trades_data['trdMatchID'],
                trades_data['side'].lower(),
                trades_data['price'],
                trades_data['size']
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
        ohlcv_list = []
        if not data:
            return ohlcv_list
        for x in data:
            fmt_k = await self.format_kline([
                await self.str_2_timestamp(x['timestamp']) - 60,
                x['open'] or 0,
                x['high'] or 0,
                x['low'] or 0,
                x['close'] or 0,
                x['volume'] or 0
            ])
            if fmt_k:
                ohlcv_list.append(fmt_k)
        return ohlcv_list

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时trade
        """
        try:
            if msg == 'pong':
                return
            data = json.loads(msg)
            if not data:
                return
            if 'data' not in data:
                return
        except Exception as e:
            await self.logger.error(e)
            return
        tick_data_list = data['data']
        for trades_data in tick_data_list:
            symbol = self.reverse_symbols[trades_data['symbol'].lower()]
            format_trade = await self.format_trade([
                await self.str_2_timestamp(trades_data["timestamp"], timedelta_hours=8),
                trades_data['trdMatchID'],
                trades_data['side'].lower(),
                trades_data['price'],
                trades_data['size']
            ])
            if not format_trade:
                await self.save_trades_to_redis(symbol, [])
                continue
            await self.save_trades_to_redis(symbol, [format_trade])
        return

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            if msg == 'pong':
                return
            data = json.loads(msg)
            if not data:
                return
            if 'data' not in data:
                return
        except Exception as e:
            await self.logger.error(f'{e} {msg}')
            return
        kline_data_list = data['data']
        for kline in kline_data_list:
            symbol = self.reverse_symbols[kline['symbol'].lower()]
            ohlcv = await self.format_kline([
                await self.str_2_timestamp(kline['timestamp']) - 60,
                kline['open'] or 0,
                kline['high'] or 0,
                kline['low'] or 0,
                kline['close'] or 0,
                kline['volume'] or 0,
            ])
            await self.save_kline_to_redis(symbol, ohlcv)

    async def get_aicoin_symbol_map(self):
        """
        功能:
            获取aicoin symbol 映射
        """
        symbol_map = {
            'xbtusd': 'xbt:bitmex',
            'ethusd': 'ethusd:bitmex',
        }
        return symbol_map
