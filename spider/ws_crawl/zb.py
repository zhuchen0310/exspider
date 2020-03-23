#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-27 18:08
import ujson

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE


class zb(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.exchange_id = 'zb'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
            },
            'api': 'http://api.zb.cn/data/v1',
            'urls': {
                'symbols': '/markets',
                'trades': '/trades?market={}',
                'klines': '/kline?market={}&type=1min'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://api.zb.cn/websocket'
            }
        }
        self.symbols = self.get_symbols()
        self.max_sub_num = 50
        self.ping_interval_seconds = None

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
        return ujson.dumps(
            {'event': 'addChannel', 'channel': f'{symbol}_trades'}
        )

    async def get_sub_depth_data(self, symbol):
        """
        功能:
            zb 没有 心跳, 所有订阅一个 频繁更新的depth
        :return:
        """
        return ujson.dumps(
            {
                'event': 'addChannel',
                'channel': f'{symbol}_depth',
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
            await ws.send_str(await self.get_sub_depth_data(pending_symbols[0]))
            if ws_type == WS_TYPE_TRADE:
                send_sub_datas.extend([await self.get_trade_sub_data(s) for s in pending_symbols])
            elif ws_type == WS_TYPE_KLINE:
                send_sub_datas.extend([await self.get_kline_sub_data(s) for s in pending_symbols])
            send_sub_datas = set(send_sub_datas)
        for sub_data in send_sub_datas:
            await ws.send_str(sub_data)

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
        if symbol not in self.symbols:
            return
        api = self.http_data['api']
        path = self.http_data['urls']['klines'].format(self.symbols[symbol])
        url = f'{api}{path}&size={self.http_data["limits"]["kline"]}'
        return url

    async def parse_restful_trade(self, data, symbol, is_save=True):
        """
        功能:
            处理 restful 返回 trade
            封装成统一格式 保存到Redis中
        返回:
            {
                "date": 1562382512,
                "amount": "0.0430",
                "price": "11317.28",
                "trade_type": "ask",
                "type": "sell",
                "tid": 505886615
              },
        """
        trade_list = []
        if not data:
            return trade_list
        for x in data:
            format_trade = await self.format_trade([
                int(x["date"]),
                x["tid"],
                'b' if x['type'].lower() == 'buy' else 's',
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
            "data": [
                {"date":1562555534,"amount":"0.0373","price":"11343.49","trade_type":"ask","type":"sell","tid":508566002}
                ]
            "dataType": "trades",
            "channel": "btcusdt_trades"
        }
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
        symbol = channel_list[0]
        trade_list = []
        for x in tick_data_list:
            format_trade = await self.format_trade([
                int(x["date"]),
                x["tid"],
                'b' if x["type"].lower() == 'buy' else 's',
                x['price'],
                x['amount']
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
                int(x[0]) // 1000,
                x[1],
                x[2],
                x[3],
                x[4],
                x[5],
            ])
            if fmt_k:
                ohlcv_list.append(fmt_k)
        return ohlcv_list

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
