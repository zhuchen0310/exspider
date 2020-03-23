import asyncio
import datetime
import re

from spider.ws_crawl import HoldBase, WS_TYPE_TRADE, WS_TYPE_KLINE
from ._base import logger
from exspider.utils.enum_con import SpiderStatus, TimeFrame


MESSAGE_SEPERATOR = chr(30)
MESSAGE_PART_SEPERATOR = chr(31)


class aicoin(HoldBase):

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None, *args, **kwargs):
        super().__init__(loop=loop, http_proxy=http_proxy, ws_proxy=ws_proxy, *args, **kwargs)
        self.ws_url = 'wss://wsv3.aicoin.net.cn/deepstream'
        self.C_CHR = f'C CHR {self.ws_url}'
        self.A_REQ = 'A REQ {"username":"visitor","password":"password"}'
        self.exchange_id = 'aicoin'
        self.http_timeout = 5
        self.ws_timeout = 5
        self.Referer = 'https://www.aicoin.net.cn/chart/{}'
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
                'Referer': "",
                'Content-Type': "application/json",
            },
            'api': 'https://www.aicoin.net.cn/api',
            'urls': {
                'symbols': '',
                'trades': '/chart/kline/data/trades',
                'klines': '/chart/kline/data/period'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://wsv3.aicoin.net.cn/deepstream'
            }
        }
        self.is_send_sub_data = False
        self.period_map = {
            TimeFrame.m1.value: '1',
            TimeFrame.m5.value: '5',
            TimeFrame.m15.value: '15',
            TimeFrame.m30.value: '30',
            TimeFrame.h1.value: '60',
            TimeFrame.h4.value: '240',
            TimeFrame.h12.value: '720',
            TimeFrame.d1.value: '1440',
            TimeFrame.w1.value: '10080',
            TimeFrame.M1.value: '43200'
        }

    async def get_ws_url(self, ws_type=None):
        """
        功能:
            生成 ws 链接
        """
        return self.ws_data['api']['ws_url']

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        return f'{self.http_data["api"]}{self.http_data["urls"]["trades"]}'

    async def get_restful_kline_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        return f'{self.http_data["api"]}{self.http_data["urls"]["klines"]}'

    async def get_restful_trades(self, symbol, is_save=True, is_first_request=False):
        """
        功能:
            获取 RESTFUL trade
        """
        params = {"symbol": symbol}
        referer = self.Referer.format('-'.join(symbol.split(':')[::-1]))
        self.http_data['headers']['Referer'] = referer

        url = await self.get_restful_trade_url(symbol)
        data = await self.get_http_data(url, request_method='POST', json=params)
        ret = await self.parse_restful_trade(data, symbol, is_save=is_save)
        return ret

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
        for trades_data in trades_data_list:
            format_trade = await self.format_trade(
                [
                    trades_data["date"],
                    trades_data["tid"],
                    trades_data["trade_type"],
                    trades_data["price"],
                    trades_data["amount"]
                ])
            if not format_trade:
                continue
            trade_list.append(format_trade)
        if is_save:
            await self.save_trades_to_redis(symbol, trade_list)
        else:
            return trade_list

    async def get_restful_klines(self, symbol, timeframe, limit=None):
        """
        功能:
            获取 RESTFUL klines
        参数:
            symbol @: 'btcusdt:huobipro'
        """
        key_data = await self.redis_con.get_aicoin_pair_data(symbol)
        params = {"symbol": key_data['k'], "period": self.period_map[timeframe], "open_time": "24"}
        referer = self.Referer.format(key_data['r'])
        self.http_data['headers']['Referer'] = referer

        url = await self.get_restful_kline_url(symbol)
        data = await self.get_http_data(url, request_method='POST', json=params)
        ret = await self.parse_restful_kline(data, limit)
        return ret

    async def parse_restful_kline(self, data, limit):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
        """
        if not data or not data.get('data', None):
            return
        if limit:
            ohlcv_list = [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in data['data']['kline_data'][-limit:]]
        else:
            ohlcv_list = [[x[0], x[1], x[2], x[3], x[4], x[5]] for x in data['data']['kline_data']]
        return ohlcv_list

    @property
    def now(self):
        return datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")

    async def get_trade_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        return f'E S trades/{symbol}'

    async def send_message(self, websocket, message):
        print(f"{self.now} < {message}")
        await websocket.send_str(message.replace(' ', MESSAGE_PART_SEPERATOR))

    async def parse_trade(self, message, websocket):
        """
        功能:
            处理 ws 实时trade
        """
        message = message.strip(MESSAGE_SEPERATOR).replace(MESSAGE_PART_SEPERATOR, ' ')
        if message.startswith('C PI'):
            await self.send_message(websocket, 'C PO')
        elif 'O[[' in message:
            symbol_data = re.findall(r'trades/(.+?) O', message)
            symbol_data = symbol_data[0].replace(' ', '') if symbol_data else None
            if not symbol_data:
                return
            re_trade = re.findall(r'(\[{2}.+?\]{2})', message)
            trade_list = []
            for trade in re_trade:
                trades = eval(trade)
                for x in trades:
                    format_trade = await self.format_trade(
                        [
                            x[0],
                            x[1],
                            x[2],
                            x[3],
                            x[4],
                        ]
                    )
                    if not format_trade:
                        continue
                    trade_list.append(format_trade)
            await self.save_trades_to_redis(symbol_data, trade_list, websocket)
        elif 'C CH' in message:
            await self.send_message(websocket, self.C_CHR)
            message = (await websocket.receive()).data
            await self.consumer(websocket, message)

            await self.send_message(websocket, 'A REQ {"username":"visitor","password":"password"}')
            message = (await websocket.receive()).data
            await self.consumer(websocket, message)
            symbol = await self.get_first_symbol(WS_TYPE_TRADE)
            if not symbol:
                return
            await self.send_message(websocket, f'E S trades/{symbol}')
        return

    async def consumer(self, websocket, message):
        message = message.strip(MESSAGE_SEPERATOR).replace(MESSAGE_SEPERATOR, ' ')
        print(f"{self.now} > {message}")
        if message.startswith('C PI'):
            await self.send_message(websocket, 'C PO')

    async def get_first_symbol(self, ws_type=WS_TYPE_TRADE):
        """
        功能:
            aicoin 首次启动从 库里取一个待启动的symbol
        返回:
            'btcusdt:huobipro'
        """
        first_symbol = await self.redis_con.get_pending_symbol_by_pid(self.exchange_id, limit=1, ws_type=ws_type)
        if not first_symbol:
            return
        symbol = first_symbol[0]
        self.crawl_symbol_map.update({
            symbol: {
                'is_first_save': True
            }
        })
        await self.redis_con.set_exchange_symbol_status(self.exchange_id, symbol, status=SpiderStatus.running.value, pid=self.uuid, ws_type=ws_type)
        return symbol

    async def send_new_symbol_sub(self, pending_symbols, ws, ws_type=WS_TYPE_TRADE):
        """
        功能:
            重连 以后 检测是否有新的订阅
            binance 这种的通过url订阅的, 只能先关闭, 再重新启动
        """
        new_send_sub_datas = []
        if ws_type == WS_TYPE_TRADE:
            new_send_sub_datas = [
                await self.get_trade_sub_data(s) for s in pending_symbols
            ] if pending_symbols else []
        elif ws_type == WS_TYPE_KLINE:
            new_send_sub_datas = [
                await self.get_kline_sub_data(s) for s in pending_symbols
            ] if pending_symbols else []
        for sub_data in new_send_sub_datas:
            await self.send_message(ws, sub_data)

    async def start_crawl(self, ws_type=WS_TYPE_TRADE):
        """
        功能:
            对外暴露的接口
            持续监听交易所设置
            如果停止: 则关闭本进程
            否则: 持续websockt 连接中, 支持断开重连, RESTFUL数据补全
        """
        await self.publisher.connect()
        while True:
            await asyncio.sleep(self.ws_timeout)
            try:
                if not await self.redis_con.get_pending_symbol_by_pid(self.exchange_id, ws_type=ws_type):
                    ...
            except:
                continue
            await logger.info(f'{self.exchange_id} Start Ws...')
            await self.get_ws_data_forever(ws_type=ws_type)
