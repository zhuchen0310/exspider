#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-29 14:24
from __future__ import absolute_import

import time
import json
import requests

from spider.ws_crawl import HoldBase, WsError


class kucoin(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'kucoin'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'Content-Type': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.kucoin.com/api/v1',
            'urls': {
                'symbols': '/symbols',
                'trades': '/market/histories?symbol={}',
                'klines': '/market/candles',
                'ws_api': '/bullet-public'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_api': '/bullet-public'
            }
        }
        self.max_sub_num = 50 # 每个连接 最大订阅数
        self.symbols = self.get_symbols()
        self.ws_id = int(time.time()*1000)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data or not data.get('data'):
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x['symbol'].replace('-', '').lower():
                x['symbol']

            for x in data['data']
        }
        return symbols

    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        proxies = {'http': self.http_proxy, 'https': self.http_proxy}
        try:
            data = requests.post('https://api.kucoin.com/api/v1/bullet-public', proxies=proxies).json()
            if not data or not data.get('data'):
                raise BaseException('Get ws url data error')
            token = data['data']['token']
            ws_api = data['data']['instanceServers'][0]['endpoint']
            url = f'{ws_api}?token={token}&connectId={self.ws_id}'
            return url
        except Exception as e:
            raise BaseException(f'Post ws api error: {e}')

    async def get_ping_data(self):
        """
        功能:
            获取 ping
        """
        return json.dumps({
            'id': await self.now_timestamp,
            'type': 'ping'
        })

    async def get_trade_sub_data(self, symbols):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        match_symbols = [self.symbols[x] for x in symbols if x in self.symbols]
        return json.dumps(
            {
                "id": self.ws_id,
                "type": "subscribe",
                "topic": f"/market/match:{','.join(match_symbols)}",
                "privateChannel": False,
                "response": True
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
        path = self.http_data['urls']['klines']
        startAt = await self.now_timestamp - 2 * 60 * 60
        endAt = await self.now_timestamp
        url = f'{api}{path}?symbol={self.symbols.get(symbol, "")}&type=1min&startAt={startAt}&endAt={endAt}'
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
        if not data or not data.get('data'):
            return trade_list
        trades_data_list = data['data']
        for x in trades_data_list:
            format_trade = await self.format_trade([
                int(x['time']) // (10 ** 9),  # 秒级时间戳
                x["sequence"],
                x["side"],
                x["price"],
                x["size"]
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
            if not data or not data.get('data'):
                return
            tick_data = data['data']
            symbol = tick_data['symbol'].replace('-', '').lower()
            format_trade = await self.format_trade([
                int(tick_data['time']) // (10 ** 9),
                tick_data["sequence"],
                tick_data['side'],
                tick_data['price'],
                tick_data['size'],
            ])
            if not format_trade:
                trade_list = []
            else:
                trade_list = [format_trade]
            await self.save_trades_to_redis(symbol, trade_list, ws)
        except Exception as e:
            print(e)

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        ohlcv_list = []
        if not data or not data.get('data'):
            return ohlcv_list
        for x in data['data']:
            ohlcv = await self.format_kline([
                x[0], x[1], x[3], x[4], x[2], x[5]
            ])
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list[::-1] # 逆序
