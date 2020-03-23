#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-27 18:08
import ujson

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE


class idax(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'idax'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36',
                'Content-Type': "application/json",
            },
            'api': 'https://openapi.idax.pro/api/v2',
            'urls': {
                'symbols': '/pairs',
                'trades': '/trades?pair={}',
                'klines': '/kline?pair={}&period=1min'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://openws.idax.pro/ws'
            }
        }
        self.symbols = self.get_symbols()
        self.max_sub_num = 50
        self.ping_interval_seconds = 5

    async def get_ping_data(self):
        return ujson.dumps({"event":"ping"})

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
        if symbol not in self.symbols:
            return await self.get_ping_data()
        channel = f'idax_sub_{self.symbols[symbol]}_trades'.lower()
        return ujson.dumps({'event': 'addChannel', 'channel': channel})

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        if symbol not in self.symbols:
            return await self.get_ping_data()
        channel = f'idax_sub_{self.symbols[symbol]}_kline_1min'.lower()
        return ujson.dumps({'event': 'addChannel', 'channel': channel})

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        if symbol not in self.symbols:
            return
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(self.symbols[symbol])
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(self.symbols[symbol])
        if limit:
            url = f'{api}{path}&size={limit}'
        else:
            url = f'{api}{path}&size={self.http_data["limits"]["kline"]}'
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
        if 'trades' not in data or not data['trades']:
            return trade_list
        trades_data_list = data['trades']
        for x in trades_data_list:
            format_trade = await self.format_trade([
                int(x["timestamp"]) // 1000,
                x["id"],
                'b' if x['maker'] in ['buy', 'Buy'] else 's',
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
            处理 ws 实时 trade
        原始数据:
            {
                "channel":"idax_sub_eth_btc_trades",
                "code":"00000",
                "data":[[2602708510000030972,"0.026225","4.673",1561625122754,"sell"],[2602708510000030973,"0.026218","4.447",1561625122754,"sell"]]}
        [Transaction number, price, volume, Clinch a deal the time, Clinch a deal the type（buy|sell）]
        """
        try:
            data = ujson.loads(msg)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('data'):
            return
        tick_data_list = data['data']
        channel_list = data['channel'].split('_')
        if 'trades' not in channel_list:
            return
        symbol = f'{channel_list[2]}{channel_list[3]}'
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                int(x[3]) // 1000,
                x[0],
                'b' if x[4] == 'buy' else 's',
                x[1],
                x[2]
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
        if not data or not data.get('kline'):
            return ohlcv_list
        for x in data['kline']:
            fmt_k = await self.format_kline([
                x[0] // 1000,
                x[1],
                x[2],
                x[3],
                x[4],
                x[5],
            ])
            if fmt_k:
                ohlcv_list.append(fmt_k)
        return ohlcv_list

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时 1min kline
        原始数据:
            {
                "channel":"idax_sub_eth_btc_kline_1min",
                "code":"00000",
                "data":["1561622640000","0.026052","0.026053","0.026046","0.026048","1.26427602"] # t, o, h, l, c, v
            }
        """
        try:
            data = ujson.loads(msg)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('data'):
            return
        kline = data['data']
        channel_list = data['channel'].split('_')
        if 'kline' not in channel_list:
            return
        symbol = f'{channel_list[2]}{channel_list[3]}'
        ohlcv = await self.format_kline([
            int(kline[0]) // 1000,
            kline[1],
            kline[2],
            kline[3],
            kline[4],
            float(kline[5]) / float(kline[4]),
        ])
        await self.save_kline_to_redis(symbol, ohlcv)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data or not data.get('pairs'):
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x.replace('_', '').lower(): x
            for x in data['pairs']
        }
        return symbols
