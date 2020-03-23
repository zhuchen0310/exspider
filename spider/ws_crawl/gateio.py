#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-26 18:15

from __future__ import absolute_import

import ujson
import asyncio

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE


class gateio(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'gateio'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://data.gateio.co',
            'urls': {
                'symbols': '/api2/1/pairs',
                'trades': '/api2/1/tradeHistory/{}',
                'klines': '/api2/1/candlestick2/{}?group_sec=60&range_hour=4'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://ws.gate.io/v3/'
            }
        }
        self.symbols = self.get_symbols()
        self.max_sub_num = 50  # 每个连接 最大订阅数
        self.symbol_last_tms = {}
        self.ping_interval_seconds = 10
        self.max_sub_num = 1 if self.ws_type and self.ws_type == WS_TYPE_KLINE else 50

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
        """
        now = await self.now_timestamp
        params = [self.symbols[x].upper() for x in symbols if self.symbols.get(x)]
        return ujson.dumps(
            {
                "id": now,
                "method": "trades.subscribe",
                "params": params
            })

    async def get_kline_sub_data(self, symbol, start=1):
        """
        功能:
            gateio 单链接只能订阅一个 采用轮训的方式请求ws
        """
        pair = self.symbols.get(symbol)
        return ujson.dumps(
            {
                "id": await self.now_timestamp,
                "method":"kline.subscribe",
                "params":[pair, 60]
            })

    async def get_ping_data(self):
        """
        功能:
            获取 ping
        """
        return ujson.dumps(
            {
                "id": await self.now_timestamp,
                "method": "server.ping",
                "params": []
            })

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
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(self.symbols.get(symbol, ''))
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
        if 'data' not in data or not data['data']:
            return trade_list
        trades_data_list = data['data']
        for x in trades_data_list:
            format_trade = await self.format_trade([
                x['timestamp'],  # 秒级时间戳
                x['tradeID'],
                x["type"],
                x["rate"],
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
            data = ujson.loads(msg)
        except Exception as e:
            return
        if 'params' not in data or not data['params']:
            return
        if len(data['params']) < 2:
            return
        tick_data_list = data['params'][1]
        symbol = data['params'][0].replace('_', '').lower()
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                int(x['time']),  # 秒级时间戳
                x['id'],
                x["type"],
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
        if not data or not data.get('data'):
            return
        ohlcv_list = []
        for x in data['data']:
            format_kline = await self.format_kline(
                [
                    int(x[0]) // 1000,
                    x[5],
                    x[3],
                    x[4],
                    x[2],
                    x[1],
                ]
            )
            if format_kline:
                ohlcv_list.append(format_kline)
        return ohlcv_list

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws kline # TODO gateio SB 单链接只能订阅一个
        """
        try:
            data = ujson.loads(msg)
        except Exception as e:
            return
        if not data.get('params'):
            return
        kline_list = data['params']
        if not kline_list or not isinstance(kline_list, list):
            return
        for kline_data in kline_list:
            fmt_kline = await self.format_kline([
                kline_data[0],
                kline_data[1],
                kline_data[3],
                kline_data[4],
                kline_data[2],
                kline_data[5],
            ])
            symbol = kline_data[7].replace('_', '').lower()
            await self.save_kline_to_redis(symbol, fmt_kline)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x.replace('_', '').lower(): x
            for x in data
        }
        return symbols

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
                send_sub_datas.extend([await self.get_kline_sub_data(symbol) for symbol in pending_symbols])
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
                await self.get_kline_sub_data(symbol)
                for symbol in pending_symbols
            ]
        for sub_data in new_send_sub_datas:
            await ws.send_str(sub_data)



