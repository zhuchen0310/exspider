#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-30 15:22

from __future__ import absolute_import

import ujson

from spider.ws_crawl import HoldBase


class bithumb(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'bithumb'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.request_time_sleep = 0.4
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.bithumb.com',
            'urls': {
                'symbols': 'https://en.bithumb.com/resources/csv/total_ticker.json',
                'trades': '/public/transaction_history/{}?count=100',
                'klines': 'https://en.bithumb.com/resources/chart/{}_xcoinTrade_01M.json'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://wss.bithumb.com/public'
            }
        }
        self.pair = 'krw'
        self.is_has_ws_api = False
        self.is_check_first_restful = False
        self.symbols = self.get_symbols()

    def get_symbols(self):
        url = self.http_data['urls']['symbols']
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            f'{x}{self.pair}'.lower():
                f'{x}/{self.pair}'.upper()
            for x in data
        }
        return symbols

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
            支持 同时多个订阅, 所以 复写 父类方法
        """
        currency = symbol.replace(self.pair, '').upper()
        return ujson.dumps(
            {
                "currency": currency,
                "service": 'transaction',
                "tickDuration": '24H'
            }
        )

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(symbol.replace(self.pair, '').upper())
        url = f'{api}{path}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        url = self.http_data['urls']['klines'].format(symbol.replace(self.pair, '').upper())
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
                await self.str_2_timestamp(x['transaction_date'], timedelta_hours=-1),  # 秒级时间戳
                x["cont_no"],
                x["type"],
                x['price'],
                x["units_traded"]
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
        # try:
        #     data = ujson.loads(msg)
        #     if data.get('type') != 'ticker' or not data.get('trade_id'):
        #         return
        # except Exception as e:
        #     return
        # tick_data = data
        # symbol = data['product_id'].replace('-', '').lower()
        # trades = [
        #     [
        #         int((datetime.datetime.strptime(
        #             tick_data["time"], '%Y-%m-%dT%H:%M:%S.%fZ' if '.' in tick_data['time']
        #             else '%Y-%m-%dT%H:%M:%SZ') + datetime.timedelta(hours=8)).timestamp()),
        #         tick_data['trade_id'],
        #         tick_data['side'],
        #         float(tick_data['price']),
        #         float(tick_data['last_size']),
        #
        #     ]
        # ]
        # trade_map = {
        #     trades[0][1]: trades
        # }
        # await self.save_trade_to_level_db(symbol, trade_map, ws)
        # return
        ...

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]

        """
        if not data:
            return
        ohlcv_list = []
        for x in data:
            ohlcv = await self.format_kline([
                int(x[0]) // 1000, x[1], x[3], x[4], x[2], x[5]
            ])
            if ohlcv:
                ohlcv_list.append(ohlcv)
        return ohlcv_list