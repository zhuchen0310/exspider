#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 19:15

from __future__ import absolute_import

import asyncio
import datetime
import ujson
import time
import requests

from django.conf import settings
import aiohttp

from exspider.utils.logger_con import get_logger
from exspider.utils.enum_con import SpiderStatus, TimeFrame, RoutingKeyIncr
from exspider.utils.redis_con import AsyncRedis
from exspider.utils.amqp_con import AioPikaPublisher

logger = get_logger(__name__, is_debug=settings.IS_DEBUG)

WS_TYPE_TRADE = settings.BASE_WS_TYPE_TRADE
WS_TYPE_KLINE = settings.BASE_WS_TYPE_KLINE

AI_COIN_EXCHANGE_ID = settings.BASE_AI_COIN_EXCHANGE_ID


class CrawlSymbol:

    is_first_save = True
    check_tms = int(time.time())
    update_tms = int(time.time())
    csv_tms = 0
    restful_kline_start_tms = 0
    last_ohlcv = []

class WsError(BaseException):
    ...

class RetryConnect(BaseException):
    """发信号, 断开 重连"""
    ...


class HoldBase:

    def __init__(self, loop=None, http_proxy=None, ws_proxy=None,
                 http_timeout=5, ws_timeout=5, publisher=None, *args, **kwargs):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.logger = logger
        self.http_proxy = http_proxy if http_proxy else settings.PROXY
        self.ws_proxy = ws_proxy if ws_proxy else settings.PROXY
        self.crawl_symbol_map = {
            # 'btcusdt': CrawlSymbol()
        }
        self.is_first_ws_connection = True  # 自动设置是否是首次ws 连接
        self.storekeeper_ws_url = settings.STOREKEEPER_WS_URI
        self.storekeeper_ws = None
        self.user_sub_symbol = set()
        self.redis_con = AsyncRedis(self.loop)
        self.uuid = f'{int(time.time() * 1000)}'
        self.ping_tms = time.time()
        self.symbols = {}
        self.publisher = publisher if publisher else AioPikaPublisher(settings.RABBIT_MQ_URL, self.loop)
        self.ws_type = kwargs.get('ws_type', None)

        self.ping_interval_seconds = 55 # 主动ping server 间隔
        self.exchange_id = ''
        self.http_timeout = http_timeout
        self.ws_timeout = ws_timeout
        self.request_time_sleep = 0.4
        self.http_data = {
            'headers': {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
            },
            'api': 'https://api.huobi.pro',
            'urls': {
                'symbols': '/v1/common/symbols',
                'trades': '/market/history/trade',
                'klines': '/market/history/kline'
            },
            'limits': {
                'kline': 200,
                'trade': 200,
            }
        }
        self.ws_data = {
            'api': {
                'ws_url': 'wss://api.huobi.pro/ws'
            }
        }
        self.is_send_sub_data = False
        self.max_sub_num = 50 # 每个连接 最大订阅数
        self.is_check_first_restful = True # 首次ws 是否需要 请求restful
        self.is_has_ws_api = True
        self.time_frame = {TimeFrame.m1.value: '1min'}    # 支持的kline timeframe
        self.debug_cache = 0 # debug 错误推送

    def requests_data(self, url, request_method='GET', **kwargs):
        """
        功能:
            requests 获取 json 数据
        """
        try:
            headers = self.http_data['headers']
            proxies = {'http': self.http_proxy, 'https': self.http_proxy}
            timeout = self.http_timeout
            data = requests.request(request_method, url, headers=headers, proxies=proxies, timeout=timeout, **kwargs).json()
        except Exception as e:
            print(f'request error: {e}')
            data = None
        return data

    async def get_http_data(self, url, request_method='GET', **kwargs):
        """
        功能:
            请求RESTFul 接口获取数据
        """
        data = None
        try:
            async with aiohttp.ClientSession(loop=self.loop, headers=self.http_data['headers']) as session:
                async with session.request(
                        method=request_method,
                        url=url,
                        proxy=self.http_proxy,
                        headers=self.http_data['headers'],
                        timeout=self.http_timeout,
                        **kwargs
                ) as response:
                    try:
                        data = await response.json()
                    except:
                        ret = await response.text()
                        data = ujson.loads(ret)
                    finally:
                        return data
        except Exception as e:
            await logger.error(f'{self.exchange_id} get_restful_data:{url} error {e}')
        finally:
            return data

    async def get_restful_data_forever(self, ws_type=WS_TYPE_TRADE, pending_symbols=None):
        """
        功能:
            一直持续请求rest api
        """
        await self.reset_crawl_symbols(pending_symbols)
        try:
            await self.set_exchange_spider_status(SpiderStatus.running.value, ws_type=ws_type, symbols=pending_symbols)
            while True:
                for symbol in pending_symbols:
                    await asyncio.sleep(self.request_time_sleep)
                    try:
                        if ws_type == WS_TYPE_TRADE:
                            await self.get_restful_trades(symbol)
                        elif ws_type == WS_TYPE_KLINE:
                            klines = await self.get_restful_klines(symbol, TimeFrame.m1.value)
                            for kline in klines[-3:]:
                                await self.save_kline_to_redis(symbol, kline)
                    except Exception as e:
                        await logger.error(f'{self.exchange_id} > {e}')
                        continue
        except KeyboardInterrupt:
            await self.redis_con.close_exchange(self.exchange_id, ws_type=ws_type)
        except Exception as e:
            await logger.error(f'{self.exchange_id} > {e}')
            return

    async def reset_crawl_symbols(self, symbol_list=None):
        """
        功能:
            初始化, 断开重连的时候 重置基础信息
        :param symbol_list:
        :return:
        """
        if self.crawl_symbol_map:
            symbol_list = (x for x in self.crawl_symbol_map)
        for symbol in symbol_list:
            if symbol in self.crawl_symbol_map:
                self.crawl_symbol_map[symbol].is_first_save = True
                self.crawl_symbol_map[symbol].last_kline = []
            else:
                self.crawl_symbol_map[symbol] = CrawlSymbol()
        else:
            await self.logger.info(f'新订阅: {self.crawl_symbol_map.keys()}')

    async def get_ws_data_forever(self, send_sub_datas=None, ws_type=WS_TYPE_TRADE, pending_symbols=None):
        """
        功能:
            建立 websocket 长连接 持续获取数据
        参数:
            type : 指 websocket 订阅的类型 [trade, kline]
        返回:
            ret: @True: 继续重连, @False: 结束当前Task
        """
        await self.reset_crawl_symbols(pending_symbols)
        ret = True
        url = await self.get_ws_url(ws_type)
        try:
            conn = aiohttp.TCPConnector(limit=0)
            async with aiohttp.ClientSession(loop=self.loop, connector=conn) as session:
                async with session.ws_connect(url,
                                              autoping=True,
                                              proxy=self.ws_proxy,
                                              timeout=self.ws_timeout,
                                              max_msg_size=0) as ws:
                    # 首次获取 任意 待启动的 symbol
                    try:
                        await self.send_the_first_sub(send_sub_datas, ws, ws_type, pending_symbols=pending_symbols)
                    except Exception as e:
                        await logger.error(f'{self.exchange_id} > {e}')
                    # 设置交易所运行状态 运行中
                    try:
                        await self.set_exchange_spider_status(SpiderStatus.running.value, ws_type=ws_type, symbols=list(self.crawl_symbol_map.keys()))
                    except Exception as e:
                        await logger.error(f'{self.exchange_id} > {e}')
                    # 发送完订阅 等待其他 task 订阅
                    while True:
                        # ping
                        _ping = await self.get_ping_data()
                        now = await self.now_timestamp

                        if now - self.ping_tms > self.ping_interval_seconds:
                            self.ping_tms = now
                            if _ping:
                                await self.logger.info(f'{self.uuid} < {_ping}')
                                await ws.send_str(_ping)
                            try:
                                # spider 运行状态
                                await self.set_exchange_spider_status(SpiderStatus.running.value, ws_type=ws_type, symbols=list(self.crawl_symbol_map.keys()))
                            except Exception as e:
                                await self.logger.error(e)

                        try:
                            msg = await ws.receive(0.001)
                        except:
                            await asyncio.sleep(0.001)
                            continue

                        if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                            # 处理 ws message
                            await self.parse_msg_manage(msg.data, ws, ws_type=ws_type)
                        elif msg.type == aiohttp.WSMsgType.PING:
                            await self.pong(ws)
                        else:
                            if msg.type == aiohttp.WSMsgType.CLOSE:
                                await logger.info(f'WSMsgType.CLOSE {msg.data}')
                                await ws.close()
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                await logger.error(f'{self.exchange_id} Error during receive {ws.exception()}')
                                # 设置交易所运行状态 异常停止
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                await logger.info(f'WSMsgType.CLOSED {msg.data}')
                            try:
                                await self.set_exchange_spider_status(SpiderStatus.error_stopped.value, ws_type=ws_type, symbols=list(self.crawl_symbol_map.keys()))
                            except Exception as e:
                                await logger.error(f'{self.exchange_id} > {e}')
                            finally:
                                return ret
                        # 启动中 检测 新增 待启动 symbol
                        try:
                            await self.check_and_send_new_symbol(ws, ws_type=ws_type)
                        except RetryConnect:
                            return ret
                        except Exception as e:
                            await logger.error(f'{self.exchange_id} > {e}')
        except Exception as e:
            await logger.error(f'{self.exchange_id} ws error: {e}')
        return ret

    async def get_ws_url(self, ws_type=WS_TYPE_TRADE):
        """
        功能:
            生成 ws 链接
        """
        pass

    async def get_trade_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        pass

    async def get_trade_ubsub_data(self, symbol):
        """
        功能:
            获取 取消订阅消息
        """
        pass

    async def get_kline_sub_data(self, symbol):
        """
        功能:
            获取 订阅消息
        """
        pass

    async def get_kline_ubsub_data(self, symbol):
        """
        功能:
            获取 取消订阅消息
        """
        pass

    async def send_the_first_sub(self, send_sub_datas, ws, ws_type=WS_TYPE_TRADE, pending_symbols=None):
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
                send_sub_datas.extend([await self.get_trade_sub_data(s) for s in pending_symbols])
            elif ws_type == WS_TYPE_KLINE:
                send_sub_datas.extend([await self.get_kline_sub_data(s) for s in pending_symbols])
            send_sub_datas = set(send_sub_datas)
        for sub_data in send_sub_datas:
            await ws.send_str(sub_data)

    async def check_and_send_new_symbol(self, ws, ws_type=WS_TYPE_TRADE):
        """
        功能:
            功能:
            重连 以后 检测是否有新的订阅
            binance 这种的通过url订阅的, 只能先关闭, 再重新启动
            @@: 1. 如果当前订阅已满 则检查更新时间 过期的重新订阅.
                2. 不满的话则检测是否有新交易对需要订阅
        """
        now = await self.now_timestamp
        now_tms = now // 60 * 60
        base_tms = now - settings.SYMBOL_AUTO_STOP_TMS
        retry_sub_symbols = [
            x for x in self.crawl_symbol_map if self.crawl_symbol_map[x].update_tms and self.crawl_symbol_map[x].update_tms <= base_tms
        ]
        if self.is_send_sub_data:
            pending_symbols = await self.get_pending_symbols(ws_type)
            retry_sub_symbols.extend(pending_symbols)
        if not retry_sub_symbols:
            return
        for s in retry_sub_symbols:
            self.crawl_symbol_map[s].is_first_save = True
            self.crawl_symbol_map[s].restful_kline_start_tms = now_tms
            self.crawl_symbol_map[s].update_tms = now
            await self.set_exchange_symbol_status(s, status=SpiderStatus.pending.value, ws_type=ws_type)
        if not self.is_send_sub_data:
            if len(retry_sub_symbols) / len(self.crawl_symbol_map) < 0.3:
                return
            await logger.info(f'{self.exchange_id} {self.uuid} {retry_sub_symbols} disconnect WS connection...')
            await self.set_exchange_spider_status(SpiderStatus.error_stopped.value, ws_type=ws_type, symbols=list(self.crawl_symbol_map.keys()))
            raise RetryConnect('断开 重连')
        else:
            await self.logger.info(f'retry sub symbols: {len(retry_sub_symbols)}')
            await self.send_new_symbol_sub(retry_sub_symbols, ws, ws_type=ws_type)

    async def send_new_symbol_sub(self, pending_symbols, ws, ws_type=WS_TYPE_TRADE):
        """
        功能:
            发送订阅
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
            await ws.send_str(sub_data)
        del pending_symbols, new_send_sub_datas

    async def pong(self, ws):
        """
        功能:
            心跳检测
        """
        pong_data = await self.get_pong_data()
        if pong_data:
            await ws.send_str(pong_data)
        else:
            await ws.pong()

    async def get_ping_data(self):
        """
        功能:
            获取 ping
        """
        return None

    async def get_pong_data(self, ping=''):
        """
        功能:
            获取 pong 参数
        """
        pass

    async def get_restful_trade_url(self, symbol):
        """
        功能:
            获取 restful 请求的url
        """
        pass

    async def get_restful_kline_url(self, symbol, timeframe, limit):
        """
        功能:
            获取 restful 请求的url 不需要 再正反序判断
        """
        pass

    async def get_restful_trades(self, symbol, is_save=True, is_first_request=False):
        """
        功能:
            获取 RESTFUL trade
            is_save 只查还是要保存
            is_first_request 是否首次连接请求, 个别交易所订阅ws 后可以返回快照, 减少一次请求
        """
        if is_first_request and not self.is_check_first_restful:
            return
        url = await self.get_restful_trade_url(symbol)
        data = await self.get_http_data(url)
        if not is_save:
            return await self.parse_restful_trade(data, symbol, is_save=is_save)
        else:
            await self.parse_restful_trade(data, symbol)

    async def get_restful_klines(self, symbol, timeframe, limit=None, is_first_request=False):
        """
        功能:
            获取 RESTFUL klines
        """
        if is_first_request and not self.is_check_first_restful:
            return
        url = await self.get_restful_kline_url(symbol, timeframe, limit)
        data = await self.get_http_data(url)
        ret = await self.parse_restful_kline(data)
        if ret and ret[0][0] > ret[-1][0]:
            ret = ret[::-1]
        return ret

    async def parse_msg_manage(self, msg, ws, ws_type=None):
        """
        功能:
            WS 消息处理管理器
        """
        if not ws_type:
            return
        if ws_type == WS_TYPE_KLINE:
            try:
                await self.parse_kline(msg, ws)
            except Exception as e:
                await logger.error(f'Parse Kline ERROR: {e}')
            # 创建一分钟kline
            # await self.create_kline_for_symbol()
        elif ws_type == WS_TYPE_TRADE:
            try:
                await self.parse_trade(msg, ws)
            except Exception as e:
                await logger.error(f'Parse Trade ERROR: {e}')

    async def parse_restful_trade(self, data, symbol, is_save=True):
        """
        功能:
            处理 restful 返回 trade
            封装成统一格式 保存到Redis中
        返回:
            [[1551760709,"10047738192326012742563","buy",3721.94,0.0235]]
        """
        pass

    async def parse_trade(self, msg, ws):
        """
        功能:
            处理 ws 实时trade
        """
        pass

    async def parse_restful_kline(self, data):
        """
        功能:
            处理 restful 返回 kline
            统一格式 ohlcv = [tms, open, high, low, close, volume]
            tms 是 秒级
            注意时间戳 顺序: 从远到近
        """
        pass

    async def parse_kline(self, msg, ws):
        """
        功能:
            处理 ws 实时kline
        """
        pass

    async def save_trades_to_redis(self, symbol: str, trade_list: list, ws=None):
        """
        功能:
            统一处理入库
        注意:
            首次入库之前请求一次 restful 补数据
        """
        try:
            now = await self.now_timestamp
            await self.set_update_tms(symbol, now)
            if not trade_list:
                return
            await self.set_exchange_symbol_status(symbol, status=SpiderStatus.running.value, ws_type=WS_TYPE_TRADE)
            if self.crawl_symbol_map[symbol].is_first_save:
                self.crawl_symbol_map[symbol].is_first_save = False
                await self.set_check_tms(symbol, now)
                await self.get_restful_trades(symbol, is_first_request=True)

            exchange_id = self.exchange_id
            await logger.debug(f'[{exchange_id}] {symbol} >> {trade_list[:2]}')
            # redis push and publish
            routing_key = await self.get_publish_routing_key(WS_TYPE_TRADE, exchange_id, symbol)
            msg = await self.get_publish_message(WS_TYPE_TRADE, exchange_id, symbol, trade_list)
            await self.publisher.publish(routing_key, msg)
            await self.redis_con.add_exchange_symbol_trades(exchange_id, symbol, trade_list)
            ex_symbol = f'{symbol}:{exchange_id}'

            if now - self.crawl_symbol_map[symbol].check_tms > settings.CHECK_TRADES_TMS:
                # set check tms
                await self.set_check_tms(symbol)
                # check data
                await self.get_restful_trades(symbol)
            if now - self.crawl_symbol_map[symbol].csv_tms > settings.WRITE_TRADE_CSV_TMS:
                await self.set_csv_tms(symbol, now)
                # 发送csv生成指令
                await self.redis_con.push_csv_direct(ex_symbol, ws_type=WS_TYPE_TRADE)
        except Exception as e:
            await self.logger.error(e)

    async def save_kline_to_redis(self, symbol: str, kline, ws=None):
        """
        功能:
            保存 满柱kline到 redis 中写csv 实时的打入MQ中(满柱频道, 实时频道)
        """
        try:
            now = await self.now_timestamp
            await self.set_update_tms(symbol, now)
            if not kline:
                return
            # 首次&断开重连 删除缓存
            await self.set_exchange_symbol_status(symbol, status=SpiderStatus.running.value, ws_type=WS_TYPE_KLINE)
            now_minute_tms = now - (now % 60)
            if self.crawl_symbol_map[symbol].is_first_save:
                self.crawl_symbol_map[symbol].is_first_save = False
                await self.set_check_tms(symbol, now)
                kline_list = await self.get_restful_klines(symbol, TimeFrame.m1.value, is_first_request=True)
                if kline_list:
                    start_check_tms = self.crawl_symbol_map[symbol].restful_kline_start_tms
                    self.crawl_symbol_map[symbol].restful_kline_start_tms = now_minute_tms
                    kline_list = kline_list if not start_check_tms else [x for x in kline_list if x[0] >= start_check_tms]
                    if now_minute_tms <= kline_list[-1][0]:
                        kline_list = kline_list[:-1]
                    await self.publish_kline(symbol, kline_list, incr=RoutingKeyIncr.history.value)
            await logger.debug(f'[{self.exchange_id}]{self.uuid} {symbol} >> {kline}')

            if not self.crawl_symbol_map[symbol].last_ohlcv:
                self.crawl_symbol_map[symbol].last_ohlcv = kline
                await self.publish_kline(symbol, [kline], incr=RoutingKeyIncr.not_full.value)
                return
            # 缓存的上一条ohlcv
            last_ohlcv = self.crawl_symbol_map[symbol].last_ohlcv
            # 历史的丢弃
            if kline[0] < last_ohlcv[0]:
                return
            # 刷新最后一条ohlcv
            self.crawl_symbol_map[symbol].last_ohlcv = kline

            if kline[0] > last_ohlcv[0]:
                # 新的一根开始, 先发布上一条, 然后下一条
                await self.publish_kline(symbol, [last_ohlcv], incr=RoutingKeyIncr.full.value)
            await self.publish_kline(symbol, [kline], incr=RoutingKeyIncr.not_full.value)

            if now - self.crawl_symbol_map[symbol].check_tms > settings.CHECK_KLINES_TMS:
                # set check tms
                await self.set_check_tms(symbol, now)
                # check data
                kline_list = await self.get_restful_klines(symbol, TimeFrame.m1.value)
                if kline_list:
                    if now_minute_tms == kline_list[-1][0]:
                        kline_list = kline_list[:-1]
                    await self.publish_kline(symbol, kline_list, incr=RoutingKeyIncr.history.value)
            if now - self.crawl_symbol_map[symbol].csv_tms > settings.WRITE_CSV_TMS:
                await self.set_csv_tms(symbol, now)
                ex_symbol = f'{symbol}:{self.exchange_id}'
                await self.redis_con.push_csv_direct(ex_symbol, ws_type=WS_TYPE_KLINE)
        except Exception as e:
            await self.logger.error(e)

    async def publish_kline(self, symbol, klines, incr: RoutingKeyIncr=RoutingKeyIncr.not_full.value):
        """
        功能:
            发布kline, 满柱的 csv + mq, 其他的 mq
        """
        if not klines:
            return
        if incr in [RoutingKeyIncr.history.value, RoutingKeyIncr.full.value]:
            await self.redis_con.add_exchange_symbol_klines(self.exchange_id, symbol, klines)
        routing_key = await self.get_publish_routing_key(WS_TYPE_KLINE, self.exchange_id, symbol, incr=incr)
        for kline in klines[-3:]:
            # await self.debug_log(symbol, kline, incr)
            publish_data = await self.get_publish_message(WS_TYPE_KLINE, self.exchange_id, symbol, kline)
            await self.publisher.publish(routing_key, publish_data)

    async def debug_log(self, symbol, kline, incr):
        if symbol != 'btcusdt':
            return
        if incr == RoutingKeyIncr.history.value:
            return
        if kline[0] < self.debug_cache:
            await logger.error(f'{kline[0]} < {self.debug_cache}')
        self.debug_cache = kline[0]

    async def is_stop_exchange(self, ws=None, ws_type=WS_TYPE_TRADE, is_check_spider=False):
        """
        功能:
            检查交易所停止信号 spider
        说明:
            aicoin 可以通过进程号来关闭爬虫
        """
        ret = False
        try:
            spider_pid = self.uuid if is_check_spider else ''
            if await self.redis_con.check_ex_spider_stop(self.exchange_id, spider_pid=spider_pid, ws_type=ws_type):
                await logger.info(f'Stop Spider {self.uuid} ...')
                ret = True
                await self.set_exchange_spider_status(SpiderStatus.stopped.value, ws_type=ws_type, symbols=list(self.crawl_symbol_map.keys()))
        except Exception as e:
            await logger.error(f'{self.exchange_id} {e}')
        return ret

    async def set_exchange_spider_status(self, status, ws_type=WS_TYPE_TRADE, symbols=None):
        """
        功能:
            设置交易所爬虫的 运行状态
        """
        await logger.info(f'Set {self.exchange_id} Spider {self.uuid} Status: {status}')
        try:
            await self.redis_con.set_exchange_spider_status(self.exchange_id, pid=self.uuid, status=status, ws_type=ws_type, symbols=symbols)
        except Exception as e:
            await logger.error(f'{self.exchange_id} Spider {self.uuid} Update Status Error {e}')

    async def set_exchange_symbol_status(self, symbol, status, ws_type=WS_TYPE_TRADE):
        """
        功能:
            设置交易所 交易对 运行状态
        """
        try:
            await self.redis_con.set_exchange_symbol_status(self.exchange_id, symbol, status, pid=self.uuid, ws_type=ws_type)
        except Exception as e:
            await logger.error(f'{self.exchange_id} {symbol} Update Status Error {e}')

    @property
    async def now_timestamp(self):
        return int(time.time())

    def close_spider(self, ws_type=WS_TYPE_TRADE):
        """
        功能:
            主动停止 交易所爬虫
        """
        loop = self.loop if not self.loop.is_closed() else asyncio.new_event_loop()
        self.loop = loop
        self.loop.run_until_complete(self.redis_con.close_exchange(self.exchange_id, ws_type=ws_type))
        return True

    async def start_crawl(self, ws_type=WS_TYPE_TRADE, pending_symbols=None):
        """
        功能:
            对外暴露的接口
            持续监听交易所设置
            如果停止: 则关闭本进程
            否则: 持续websockt 连接中, 支持断开重连, RESTFUL数据补全
        """
        await self.publisher.connect()
        while True:
            if await self.is_stop_exchange(ws_type=ws_type, is_check_spider=False):
                await logger.info(f'{self.exchange_id} {self.uuid} Spider Stop...')
                return
            await logger.info(f'{self.exchange_id} {self.uuid} Start Ws...')
            if self.is_has_ws_api:
                if not await self.get_ws_data_forever(ws_type=ws_type, pending_symbols=pending_symbols):
                    await logger.info(f'{self.exchange_id} {self.uuid} have not symbols...')
                    return
            else:
                await self.get_restful_data_forever(ws_type=ws_type, pending_symbols=pending_symbols)
            # 断开暂停5秒重连
            await asyncio.sleep(self.ws_timeout)

    async def set_check_tms(self, symbol, now=None):
        """
        功能:

            设置本次校验时间戳
        """
        now = now if now else await self.now_timestamp
        self.crawl_symbol_map[symbol].check_tms = now

    async def set_update_tms(self, symbol, now=None):
        """
        功能:

            设置本次更新时间戳
        """
        now = now if now else await self.now_timestamp
        self.crawl_symbol_map[symbol].update_tms = now

    async def set_csv_tms(self, symbol, now=None):
        """
        功能:

            设置本次csv保存时间戳
        """
        now = now if now else await self.now_timestamp
        self.crawl_symbol_map[symbol].csv_tms = now

    async def str_2_timestamp(self, time_str, is_timedelta=True, timedelta_hours=8):
        """
        功能:
            时间str > 时间戳 秒级时间戳
        """
        format = '%Y-%m-%d %H:%M:%S'
        if 'T' in time_str:
            format = '%Y-%m-%dT%H:%M:%S'
        if '.' in time_str:
            format = f'{format}.%f'
        if 'Z' in time_str:
            format = f'{format}Z'
        if is_timedelta:
            tms = int((datetime.datetime.strptime(
                time_str, format) + datetime.timedelta(hours=timedelta_hours)).timestamp())
        else:
            tms = int((datetime.datetime.strptime(
                time_str, format)).timestamp())
        return tms

    def get_symbols(self):
        """
        功能:
            初始化 交易所 的 所有交易对
        """
        return {}

    async def format_trade(self, trade: list) -> list:
        """
        功能:
            格式化 trade 为统一格式
            [
                1111 ,      ----> timestamp int 秒
                '1211' ,    ----> trade_id str
                's' ,       ----> type str
                3400.3 ,    ----> price float
                0.3 ,       ----> amount float
            ]
        """
        try:
            ret_trade = [
                int(float(trade[0])) if not isinstance(trade[0], int) else trade[0],
                f'{trade[1]}',
                'b' if f'{trade[2]}' in ['buy', 'bid', 'b', 'BUY', 'BID', 'Buy', 'Bid'] else 's',
                float(trade[3]),
                float(trade[4]),
            ]
            return ret_trade
        except Exception as e:
            await logger.error(f'{self.exchange_id} format trade error: {e}')
            return []

    async def format_kline(self, kline: list) -> list:
        """
        功能:
            格式化 trade 为统一格式
            [
                1111 ,      ----> timestamp int 秒
                1 ,         ----> open float
                2 ,         ----> high float
                3 ,         ----> low float
                4 ,         ----> close float
                5 ,         ----> volume float
            ]
        """
        try:
            ret_kline = [
                int(float(kline[0])) if not isinstance(kline[0], int) else kline[0],
                float(kline[1]),
                float(kline[2]),
                float(kline[3]),
                float(kline[4]),
                float(kline[5]),
            ]
            return ret_kline
        except Exception as e:
            await logger.error(f'{self.exchange_id} format kline error: {e}, {kline}')
            return []

    async def get_publish_routing_key(self, type, exchange_id, symbol, incr: RoutingKeyIncr=RoutingKeyIncr.not_full.value):
        """
        功能:
            获取 mq routing_key
        实例:
            "market.ticker.okex.btc.usdt.spot.0.0"
        """
        base, quote = await self.get_symbol_base_and_quote(symbol)
        routing_key = f"market.{type}.{exchange_id}.{base}.{quote}.spot.0.{incr}"
        return routing_key

    async def get_publish_message(self, type, exchange_id, symbol, data):
        """
        功能:
            封装 publish_data
            'c': 'kline',                                                                   # channel
            'e': 'huobipro',                                                                # 交易所id
            't': 'timestamp',                                                               # 时间戳
            's': 'btcusdt',                                                                 # 交易对
            'd': [1555289520, 5131.96, 5134.23, 5131.96, 5133.76, 10.509788169770346]       # 数据
        """
        info = {
            'c': type,
            'e': exchange_id,
            't': await self.now_timestamp,
            's': symbol,
            'd': data
        }
        return ujson.dumps(info)

    async def get_symbol_base_and_quote(self, symbol):
        """
        功能:
            获取一个交易对的 base, quote

        :param symbol: btcusdt
        :return: btc, usdt or None
        """
        _symbol = self.symbols.get(symbol, None)
        if not _symbol:
            return symbol, None
        if '/' in _symbol:
            split_str = '/'
        elif '-' in _symbol:
            split_str = '-'
        elif '_' in _symbol:
            split_str = '_'
        else:
            split_str = _symbol
        try:
            base, quote =  _symbol.lower().split(split_str)
            return base, quote
        except:
            return symbol, None

    async def get_pending_symbols(self, ws_type=WS_TYPE_TRADE):
        """
        功能:
            获取 待启动的交易对 设置为启动中
            不能超过 交易所最大订阅数
        """
        try:
            if len(self.crawl_symbol_map) < self.max_sub_num:
                limit = self.max_sub_num - len(self.crawl_symbol_map)
                symbols = await self.redis_con.get_pending_symbol_by_pid(
                    self.exchange_id,
                    limit=limit,
                    pid='', ws_type=ws_type)
            else:
                return []
            # 添加首次 校验时间
            for symbol in symbols:
                await self.set_exchange_symbol_status(symbol, SpiderStatus.running.value, ws_type)
                if symbol in self.crawl_symbol_map:
                    continue
                self.crawl_symbol_map[symbol] = CrawlSymbol()
            return symbols
        except Exception as e:
            await logger.error(f'{self.exchange_id} {e}')
            return []