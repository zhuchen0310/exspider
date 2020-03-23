#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-25 16:36

from __future__ import absolute_import

import json
import ujson

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE


class bitfinex(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'bitfinex'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36'
            },
            'api': 'https://api.bitfinex.com/v1',
            'urls': {
                'symbols': '/symbols',
                'trades': '/trades/{}',
                'klines': 'https://api.bitfinex.com/v2/candles/trade:1m:t{}/hist'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://api.bitfinex.com/ws'
            }
        }
        self.symbols = self.get_symbols()
        self.chan_id_map = {}
        self.last_kline_tms = 0
        self.pair_cache = {}
        self.max_sub_num = 20

    async def get_ws_url(self, ws_type=WS_TYPE_TRADE):
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
        return json.dumps(
            {
                "event": "subscribe",
                "channel": "trades",
                "pair": f"{symbol.upper()}"
            }
        )

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        return json.dumps(
            {
                "event": "subscribe",
                "channel": "candles",
                "key": f"trade:1m:t{symbol.upper()}"
            }
        )

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['trades'].format(symbol)
        url = f'{api}{path}?limit_trades={self.http_data["limits"]["trade"]}'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        url = self.http_data['urls']['klines'].format(symbol.upper())
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
                int(x["timestamp"]),  # 秒级时间戳
                x["tid"],
                x["type"],
                x["price"],
                x["amount"]
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
            result = ujson.loads(msg)
            if 'hb' in result:
                await ws.send_json({
                    "event": "ping",
                    "cid": result[0]
                })
                return
            if 'tu' not in result:
                return
        except Exception as e:
            await self.logger.error(e, msg)
            return
        symbol = result[2].split('-')[-1].lower()
        format_trade = await self.format_trade([
            int(result[4]),
            result[3],
            'sell' if float(result[6]) < 0 else 'buy',
            result[5],
            abs(float(result[6])),
        ])
        if not format_trade:
            return
        trade_list = [format_trade]
        await self.save_trades_to_redis(symbol, trade_list, ws)
        return

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        ohlcv_list = []
        if not data or 'error' in data:
            return ohlcv_list
        for x in data:
            fmt_kline = await self.format_kline([
                x[0] // 1000,
                x[1],
                x[3],
                x[4],
                x[2],
                x[5]
            ])
            if fmt_kline:
                ohlcv_list.append(fmt_kline)
        return ohlcv_list[::-1] # 从远到近排列

    async def parse_kline(self, msg, ws):
        """
        处理 kline
        """
        try:
            result = json.loads(msg)
            if not result:
                return
            if result and isinstance(result, dict) and result.get('event', '') == 'subscribed':
                self.chan_id_map[result['chanId']] = result['key'].split(
                    ':')[2].replace('t', '').lower()
                return
            # ping pong
            if result and isinstance(result, list) and result[1] == "hb":
                await ws.send_json({"event":"ping", "cid": result[0]})
                return

            if isinstance(result, list):
                sub_id = result[0]
                # 先保存初始化的kline
                if isinstance(result[1][0], list) and result[1][0]:
                    kline_data = result[1][0]
                elif isinstance(result[1], list):
                    kline_data = result[1]
                else:
                    return
            else:
                return
        except Exception as e:
            await self.logger.error(e, msg)
            return
        try:
            if not kline_data or not isinstance(kline_data, list):
                return
            timestamp = int(kline_data[0]) // 1000
            if not self.last_kline_tms:
                self.last_kline_tms = timestamp
            if timestamp < self.last_kline_tms:
                return
            else:
                self.last_kline_tms = timestamp
            symbol = self.chan_id_map.get(sub_id, '')
            if not symbol:
                return
            ohlcv = await self.format_kline([
                timestamp,
                kline_data[1],
                kline_data[3],
                kline_data[4],
                kline_data[2],
                kline_data[5],
            ])
            await self.save_kline_to_redis(symbol, ohlcv)
        except Exception as e:
            await self.logger.error(e, kline_data)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x.lower():
                x.upper()
            for x in data
        }
        return symbols

    async def get_symbol_base_and_quote(self, symbol):
        """
        功能:
            获取一个交易对的 base, quote

        :param symbol: btcusdt
        :return: btc, usdt or None
        """
        if not self.pair_cache:
            await self.init_pair_cache
        _symbol = self.symbols.get(symbol, None)
        if not _symbol:
            return symbol, None
        try:
            base, quote = _symbol.lower().split('/')
            return base, quote
        except:
            return symbol, None

    @property
    async def init_pair_cache(self):
        """
        生成
        :return:
        """
        coin_list = ['btc', 'usdt', 'eur', 'gbp', 'jpy', 'eth', 'eos', 'xlm', 'dai', 'usd', 'ust']

        for symbol in self.symbols:
            for coin in coin_list:
                if coin in symbol:
                    b, q = symbol.split(coin)
                    base = b or coin
                    quote = q or coin
                    self.pair_cache[symbol] = f"{base}/{quote}"
                    break
        return self.pair_cache

