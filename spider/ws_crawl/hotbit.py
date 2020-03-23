#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 16:15


from __future__ import absolute_import

import json
import zlib

from spider.ws_crawl import HoldBase, WS_TYPE_KLINE, WS_TYPE_TRADE


class hotbit(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'hotbit'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.hotbit.io/api/v1',
            'urls': {
                'symbols': '/market.list',
                'trades': '/market.deals',
                'klines': '/market.kline'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://ws.hotbit.io'
            }
        }
        self.is_open_aicoin = True
        self.symbols = self.get_symbols()
        self.ping_interval_seconds = 20
        self.max_sub_num = 1 if self.ws_type and self.ws_type == WS_TYPE_KLINE else 100     # 这就很伤 hotbit 币多 单链接只能订阅一个 气人不?

    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        return self.ws_data['api']['ws_url']

    async def get_ping_data(self):
        return json.dumps({
                "method": "server.ping",
                "params": [],
                "id": 104
            })

    async def get_trade_sub_data(self, symbols):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        params = [symbol.upper() for symbol in symbols]
        return json.dumps({
            "method": "deals.subscribe",
            "params": params,
            "id": 18
        })

    async def get_kline_sub_data(self, symbol, start=None):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        return json.dumps({
            "method": "kline.subscribe",
            "params": [symbol.upper(), 60],
            "id": 501
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
                send_sub_datas = set(send_sub_datas)
            elif ws_type == WS_TYPE_KLINE:
                send_sub_datas = [await self.get_kline_sub_data(symbol) for symbol in pending_symbols]
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
            new_send_sub_datas = [await self.get_kline_sub_data(symbol) for symbol in pending_symbols]
        for sub_data in new_send_sub_datas:
            await ws.send_str(sub_data)

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades']
        if symbol not in self.symbols:
            raise BaseException(f'{self.exchange_id} {symbol}')
        params_symbol = self.symbols[symbol]
        url = f'{api}{path}?market={params_symbol}&limit={self.http_data["limits"]["trade"]}&last_id=0'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        now = await self.now_timestamp
        start_time = now - 3 * 60 * 60
        if symbol not in self.symbols:
            raise BaseException(f'{self.exchange_id} {symbol}')
        params_symbol = self.symbols[symbol]
        api = self.http_data['api']
        path = self.http_data['urls']['klines']
        url = f'{api}{path}?market={params_symbol}&start_time={start_time}&end_time={now}&interval=60'
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
        if 'result' not in data:
            return trade_list
        tick_data_list = data['result']
        for x in tick_data_list:
            format_trade = await self.format_trade([
                int(x['time']),
                x['id'],
                x['type'],
                x['price'],
                x['amount'],
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
            alib_msg = zlib.decompress(msg, 16 + zlib.MAX_WBITS)
            data = json.loads(alib_msg)
            if not data:
                return
            if 'params' not in data or not isinstance(data['params'], list) or not data['params'] :
                return
        except Exception as e:
            return
        tick_data_list = data['params'][1]
        try:
            symbol = data['params'][0].lower()
        except:
            return
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                int(x['time']),
                x['id'],
                x['type'],
                x['price'],
                x['amount'],
            ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        await self.save_trades_to_redis(symbol, trade_list)
        return

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        if not data or not data.get('result'):
            return []
        ohlcv_list = []
        for x in data['result']:
            ohlcv = await self.format_kline([x[0], x[1], x[3], x[4], x[2], x[5]])
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        """
        try:
            result = zlib.decompress(msg, 16 + zlib.MAX_WBITS)
            data = json.loads(result)
        except Exception as e:
            await self.logger.error(f'{msg} error: {e}')
            return
        if not data.get('method') or data['method'] != 'kline.update':
            return
        kline_list = data['params']
        for kline in kline_list:
            symbol = kline[7].lower()
            ohlcv = await self.format_kline([
                kline[0],
                kline[1],
                kline[3],
                kline[4],
                kline[2],
                kline[5],
            ])
            await self.save_kline_to_redis(symbol, ohlcv)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data or not data.get('result'):
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x['name'].lower():
                f"{x['stock']}/{x['money']}".upper()

            for x in data['result']
        }
        return symbols