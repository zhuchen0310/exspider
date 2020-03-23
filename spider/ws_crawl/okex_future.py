#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-25 16:15


from __future__ import absolute_import

import json
import zlib

from spider.ws_crawl import WS_TYPE_TRADE, WS_TYPE_KLINE
from .future_base import WSFutureBase

class okex_future(WSFutureBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'okex_future'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36',
                'Content-Type': 'application/json'
            },
            'api': 'https://www.okex.com',
            'urls': {
                'symbols': '/api/futures/v3/instruments',
                'trades': '/api/futures/v3/instruments/{}/trades',
                'klines': '/api/futures/v3/instruments/{}/candles',
                'bursts': '/api/futures/v3/instruments/{}/liquidation?status=1'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://real.okex.com:10442/ws/v3'
            }
        }
        self.ping_interval_seconds = 30
        self.max_sub_num = 100
        self.future_type_map = {
            'this_week': 'weekly',
            'next_week': 'bi-weekly',
            'quarter': 'quarterly',
        }
        self.symbols = self.get_symbols()


    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        return self.ws_data['api']['ws_url']

    async def get_ping_data(self):
        """
        功能:
            获取 ping
        """
        return 'ping'

    async def get_trade_sub_data(self, symbols):
        """
        功能:
            获取 订阅消息
            支持 同时多个订阅, 所以 复写 父类方法
        """
        params = ["futures/trade:{}".format(symbol) for symbol in symbols]
        return json.dumps({
            "op": "subscribe",
            "args": params,
        })

    async def get_kline_sub_data(self, symbols):
        """
        功能:
            初始化 订阅所有交易对的参数
        # 订阅 KLine 数据
        {"op": "subscribe", "args": ["spot/candle60s:BTC-USD"]}
        """
        params = ["futures/candle60s:{}".format(symbol) for symbol in symbols]
        return json.dumps({
            "op": "subscribe",
            "args": params,
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
        重要:
            因为合约 为每周交割, 所以 发送新订阅时, 重新初始化symbol
        """
        self.symbols = self.get_symbols()
        new_crawl_symbol = {x: self.crawl_symbol_map[x] for x in self.crawl_symbol_map if x in self.symbols}
        self.crawl_symbol_map = new_crawl_symbol
        pending_symbols = [x for x in pending_symbols if x in new_crawl_symbol]
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
        path = self.http_data['urls']['trades'].format(symbol)
        url = f'{api}{path}?limit=200'
        return url

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(symbol)
        url = f'{api}{path}?granularity=60'
        return url

    async def get_restful_burst_url(self, symbol, start_id=None, end_id=None):
        """
        功能:
            获取 restful 请求的url
        """
        api = self.http_data['api']
        path = self.http_data['urls']['bursts'].format(symbol)
        url = f'{api}{path}'
        if end_id:
            url = f'{url}&from={end_id}'
        elif start_id:
            url = f'{url}&to={start_id}'
        return url

    async def parse_restful_trade(self, data, symbol, is_save=True):
        """
        功能:
            处理 restful 返回 trade
            封装成统一格式 保存到Redis中
        返回:
            [[1551760709,"10047738192326012742563","s",3721.94,0.0235]]
        """
        trade_list = []
        if not data:
            return trade_list
        tick_data_list = data
        for x in tick_data_list:
            format_trade = await self.format_trade([
                await self.str_2_timestamp(x["timestamp"]),
                x['trade_id'],
                x['side'],
                x['price'],
                x['qty'],
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
            decompress = zlib.decompressobj(
                -zlib.MAX_WBITS  # see above
            )
            inflated = decompress.decompress(msg)
            inflated += decompress.flush()
            data = json.loads(inflated)
            if not data:
                return
            if 'data' not in data:
                return
        except Exception as e:
            return
        tick_data_list = data['data']
        symbol = tick_data_list[0]['instrument_id']
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                await self.str_2_timestamp(x["timestamp"]),
                x['trade_id'],
                x['side'],
                x['price'],
                x['qty'],
            ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        await self.save_trades_to_redis(symbol, trade_list)

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        ohlcv_list = []
        if not data:
            return ohlcv_list
        for x in data[1:]: # 第一条是最新的 舍弃
            fmt_kline = await self.format_kline(
                [
                    await self.str_2_timestamp(x[0]),
                    x[1],
                    x[2],
                    x[3],
                    x[4],
                    x[6]
                ]
            )
            if fmt_kline:
                ohlcv_list.append(fmt_kline)
        return ohlcv_list[::-1]

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws kline
        """
        try:
            decompress = zlib.decompressobj(
                -zlib.MAX_WBITS  # see above
            )
            inflated = decompress.decompress(msg)
            inflated += decompress.flush()
            data = json.loads(inflated)
            if 'errorCode' in data and data['errorCode'] in [30040, '30040']:
                return data['message']
            if 'data' not in data:
                return
        except Exception as e:
            data = None
        if not data:
            return
        try:
            k_data = data['data'][0]
            kline = k_data['candle']
        except:
            return
        timestamp = await self.str_2_timestamp(kline[0])
        symbol = k_data['instrument_id']
        if not symbol:
            return
        ohlcv = await self.format_kline([
            timestamp,
            kline[1],
            kline[2],
            kline[3],
            kline[4],
            kline[6],
        ])
        await self.save_kline_to_redis(symbol, ohlcv)

    def get_symbols(self):
        api = self.http_data['api']
        path = self.http_data['urls']['symbols']
        url = f'{api}{path}'
        data = self.requests_data(url)
        if not data:
            raise BaseException(f'{self.exchange_id} get symbols error')
        symbols = {
            x['instrument_id']: f'{x["underlying_index"]}_{x["quote_currency"]}_{self.future_type_map[x["alias"]]}'.lower()
            for x in data
        }
        return symbols

    async def parse_restful_burst(self, data):
        """
        功能:
            处理 爆仓订单
        burst = [
            t,
            print,
            amount,
            type,   # long 多/ short空
            loss
        ]
        """
        burst_list = []
        if not data or not isinstance(data, list):
            return burst_list
        for x in data:
            t = await self.str_2_timestamp(x['created_at'])
            burst_list.append(
                [
                    t,
                    float(x['price']),
                    float(x['size']),
                    'l' if x['type'] == '3' else 's',
                    float(x['loss'])
                ]
            )
        return burst_list[::-1]