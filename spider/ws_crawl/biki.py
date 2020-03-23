#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-27 18:08
import ujson
import gzip

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE


class biki(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'biki'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36',
                'Content-Type': "application/json",
            },
            'api': 'https://openapi.biki.com/open/api',
            'urls': {
                'symbols': '/common/symbols',
                'trades': '/get_trades?symbol={}',
                'klines': '/get_records?symbol={}&period=1'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://ws.biki.com/kline-api/ws'
            }
        }
        self.symbols = self.get_symbols()
        self.max_sub_num = 50
        self.ping_interval_seconds = 5

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
        return ujson.dumps({
            "event": "sub",
            "params": {
                "channel": f"market_{symbol}_trade_ticker",
                "cb_id": symbol,
                "top": 100
            }
        })

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        return ujson.dumps({
            "event":"sub",
            "params":{
                "channel": f"market_{symbol}_kline_1min",
                "cb_id": symbol
            }
        })

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(symbol)
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(symbol)
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
            format_trade = await self.format_trade([
                int(x["ctime"]) // 1000,
                x["id"],
                'b' if x['type'].lower() in ['buy', 'b'] else 's',
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
            处理 ws 实时 trade
        原始数据:
        {
            'event_rep': '',
            'channel': 'market_vdsusdt_trade_ticker',
            'data': None,
            'tick': {
                'data': [
                    {
                        'amount': '166.679526',
                        'ds': '2019-06-28 11:55:05',
                        'id': 824233,
                        'price': '3.4374',
                        'side': 'SELL',
                        'ts': 1561694105000,
                        'vol': '48.49'
                    }],
                'ts': 1561694105000
            },
            'ts': 1561694105000,
            'status': 'ok'
        }

        """
        try:
            result = gzip.decompress(msg).decode()
            if 'ping' in result:
                await ws.send_str(result.replace('ping', 'pong'))
            data = ujson.loads(result)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('tick'):
            return
        tick_data_list = data['tick']['data']
        channel_list = data['channel'].split('_')
        if 'trade' not in channel_list:
            return
        symbol = channel_list[1]
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                float(x["ts"]) // 1000,
                x["id"],
                'b' if x["side"].lower() == 'bug' else 's',
                x['price'],
                x['vol']
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
        if not data or not data.get('data'):
            return ohlcv_list
        for x in data['data']:
            fmt_k = await self.format_kline([
                x[0],
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
                "event_rep":"",
                "channel":"market_vdsusdt_kline_1min",
                "data":null,
                "tick":{
                    "amount":1868.736778,
                    "close":3.4601,
                    "ds":"2019-06-28 11:31:00",
                    "high":3.4699,
                    "id":1561692660,
                    "low":3.4601,
                    "open":3.4699,
                    "tradeId":824102,
                    "vol":539.7},
                "ts":1561692682000,"status":"ok"}
        """
        try:
            result = gzip.decompress(msg).decode()
            if 'ping' in result:
                await ws.send_str(result.replace('ping', 'pong'))
            data = ujson.loads(result)
        except Exception as e:
            print(e)
            return
        if not data or not data.get('tick'):
            return
        kline = data['tick']
        channel_list = data['channel'].split('_')
        if 'kline' not in channel_list:
            return
        symbol = channel_list[1]
        ohlcv = await self.format_kline([
            int(kline["id"]),
            kline["open"],
            kline["high"],
            kline["low"],
            kline["close"],
            kline["vol"],
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
            x['symbol'].replace('_', '').lower(): f'{x["base_coin"]}/{x["count_coin"]}'
            for x in data['data']
        }
        return symbols
