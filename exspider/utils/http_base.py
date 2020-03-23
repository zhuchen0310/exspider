#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-16 16:02

import asyncio
import time

import aiohttp
import ujson

from exspider.utils.logger_con import get_logger


logger = get_logger(__name__, is_debug=True)


class HttpBase:

    def __init__(self, loop=None, proxy=None, timeout=5):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.headers = {}
        self.proxy = proxy
        self.timeout = timeout

    async def get_http_response(self, url, request_method='GET', is_json=True, **kwargs):
        """
        功能:
            请求RESTFul 接口获取数据
        """
        data = None
        if 'headers' in kwargs:
            self.headers = kwargs['headers']
        try:
            async with aiohttp.ClientSession(loop=self.loop, headers=self.headers) as session:
                async with session.request(
                        method=request_method,
                        url=url,
                        proxy=self.proxy,
                        headers=self.headers,
                        timeout=self.timeout,
                        **kwargs) as response:
                    if is_json:
                        try:
                            data = await response.json()
                        except Exception as e:
                            ret = await response.text()
                            data = ujson.loads(ret)
                            await logger.error(e)
                    else:
                        data = await response.text()
        except Exception as e:
            await logger.error(e)
        finally:
            return data

    async def get_json_data(self, url, request_method='GET', **kwargs):
        """
        功能:
            返回 json 数据
        """
        return await self.get_http_response(url, request_method, **kwargs)

    async def get_data(self, url, request_method='GET', **kwargs):
        """
        功能:
            返回 字符串 数据
        """
        return await self.get_http_response(url, request_method, is_json=False, **kwargs)


class WSBase:
    """
    使用:
        参数:
            url = 'wss://ws.hotbit.io'
            proxy = '127.0.0.1:1081'
            sub_data = {
                "method": "kline.subscribe",
                "params": ["BTCUSDT", 60],
                "id": 100
            }
        发送:
            ws = WSBase(proxy=proxy)
            ws.ping_interval_seconds = 5 # 自动ping时间
            await ws.add_sub_data(sub_data) # 支持添加多个 *sub_data
            await ws.get_ws_data_forever(url, sub_data)
    说明:
        属性:
            proxy @:                     http代理
            timeout @:                   超时时间
            ping_interval_seconds @:     主动ping 的定时时间

        方法:
            复写on_message 方法, 进行消息处理
            复写get_ping_data 方法, 自定义发送ping数据

    """

    def __init__(self, loop=None, proxy=None, timeout=5):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.headers = {}
        self.proxy = proxy
        self.timeout = timeout
        self.max_sub_num = 20                            # 单连接 最大订阅数量
        self.ping_interval_seconds = 60                  # 主动ping 的定时时间
        self._last_ping_tms = 0                          # 上次ping 的时间
        self._pending_sub_data = []
        self._sub_cache = []

    async def get_ws_data_forever(self, url):
        conn = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(loop=self.loop, connector=conn) as session:
            async with session.ws_connect(url, proxy=self.proxy) as ws:
                await self.on_open(ws)
                while True:
                    await self.send_sub_data(ws)
                    try:
                        msg = await ws.receive(timeout=self.timeout)
                    except Exception as e:
                        continue
                    finally:
                        await self.ping(ws)

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self.on_message(ws, msg.data.strip())
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await self.on_message(ws, msg.data)
                    elif msg.type == aiohttp.WSMsgType.PING:
                        await self.pong(ws)
                    elif msg.type == aiohttp.WSMsgType.PONG:
                        await logger.info('Pong received')
                    else:
                        if msg.type == aiohttp.WSMsgType.CLOSE:
                            await ws.close()
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            await self.on_error(ws, f'{ws.exception()}')
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            await self.on_close(ws)
                        break

    async def on_open(self, ws):
        await logger.info('Start...')

    async def on_message(self, ws, message):
        await logger.info(f'> {message}')

    async def on_error(self, ws, error):
        await logger.error(error)

    async def on_close(self, ws):
        await logger.info("### closed ###")

    async def pong(self, ws):
        await ws.pong()

    async def ping(self, ws):
        """
        功能:
            客户端 主动ping
        """
        if not self.ping_interval_seconds:
            return
        ping_data = await self.get_ping_data()
        now = await self.now_timestamp
        if now - self._last_ping_tms > self.ping_interval_seconds:
            self._last_ping_tms = now
            await self.send(ws, ping_data)

    async def add_sub_data(self, sub_data, *args):
        """
        功能:
            需要起一个新的task 用来动态订阅
        """
        msg = 'success'
        success = True
        if isinstance(sub_data, list):
            sub_data_list = sub_data
        else:
            sub_data_list = [sub_data] if not args else [sub_data] + list(args)
        add_nums = 0
        for sub_data in sub_data_list:
            hash_sub_data = hash(f'{sub_data}')
            if hash_sub_data in self._sub_cache:
                self._pending_sub_data.append(sub_data)
                add_nums += 1
            elif len(self._sub_cache) >= self.max_sub_num:
                success = False
                continue
            else:
                self._pending_sub_data.append(sub_data)
                add_nums += 1
        if not success:
            msg = f'This client is sub full! This time add count: {add_nums}'
        return {
            'msg': msg,
            'success': success
        }

    async def send_sub_data(self, ws):
        if self._pending_sub_data:
            sub_data = self._pending_sub_data.pop()
            await self.send(ws, sub_data)
            hash_sub_data = hash(f'{sub_data}')
            if hash_sub_data not in self._sub_cache:
                self._sub_cache.append(hash_sub_data)

    async def send(self, ws, send_data=None):
        """
        功能:
            发送 数据
        """
        if not send_data:
            await ws.ping()
            return
        await logger.info(f'< {send_data}')
        if isinstance(send_data, str):
            await ws.send_str(send_data)
        elif isinstance(send_data, dict):
            await ws.send_json(send_data)
        else:
            await ws.ping()
        if self._pending_sub_data:
            await self.send_sub_data(ws)

    async def get_ping_data(self):
        """
        功能:
            获取 ping 数据
        """
        return None

    @property
    async def now_timestamp(self):
        return int(time.time())
