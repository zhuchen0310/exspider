#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-27 15:57

from __future__ import absolute_import

import datetime
import json

from spider.ws_crawl import HoldBase


class coinbasepro(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'coinbasepro'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.pro.coinbase.com',
            'urls': {
                'symbols': '/products',
                'trades': '/products/{}/trades',
                'klines': '/products/{}/candles?granularity=60'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://ws-feed.pro.coinbase.com'
            }
        }
        self.symbols = self.get_symbols()


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
        product_ids = [self.symbols[x] for x in symbols if x in self.symbols]
        return json.dumps(
            {
                "type": "subscribe",
                "product_ids": product_ids,
                "channels": [
                    "ticker",
                    "heartbeat",
                ]
            }
        )

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
            send_sub_datas.append(
                await self.get_trade_sub_data(pending_symbols)
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
        new_send_sub_datas = [
            await self.get_trade_sub_data(pending_symbols)
        ] if pending_symbols else []
        for sub_data in new_send_sub_datas:
            await ws.send_str(sub_data)

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(self.symbols.get(symbol, ''))
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
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
        trades_data_list = data
        for x in trades_data_list:
            format_trade = await self.format_trade([
                await self.str_2_timestamp(x["time"]),
                x['trade_id'],
                x['side'],
                x['price'],
                x['size'],
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
            if data.get('type') != 'ticker' or not data.get('trade_id'):
                return
        except Exception as e:
            return
        tick_data = data
        symbol = data['product_id'].replace('-', '').lower()
        format_trade = await self.format_trade([
            await self.str_2_timestamp(tick_data["time"]),
            tick_data['trade_id'],
            tick_data['side'],
            tick_data['price'],
            tick_data['last_size'],
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

            [
                [ time, low, high, open, close, volume ],
                [ 1415398768, 0.32, 4.2, 0.35, 4.2, 12.3 ],
                ...
            ]

        """
        if not data:
            return
        ohlcv_list = []
        for x in data:
            ohlcv = await self.format_kline(
                [
                    x[0], x[3], x[2], x[1], x[4], x[5]
                ]
            )
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list[::-1]

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x['id'].replace('-', '').lower():
                x['id']

            for x in data
        }
        return symbols