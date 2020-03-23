#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:59
import time
import asyncio
import websockets
import datetime

from websockets.exceptions import ConnectionClosed
from django.conf import settings
from django.core.management import BaseCommand
from exspider.utils.enum_con import WsType
from exspider.utils.redis_con import REDIS_CON

message_seperator = settings.MESSAGE_SEPERATOR
message_part_seperator = settings.MESSAGE_PART_SEPERATOR

storekeeper_ws_conf = settings.STOREKEEPER_WS_CONF

# 记录用户订阅的交易对
WEBSOCKETS_USER_SUB_MAP = {
    'trades': {
        # 'btcusdt:huobipro': {ws1, ws2}
    }
}

# 缓存 spider ws
SPIDER_WEBSOCKETS_MAP = {}
# 记录需要发送订阅到spider 交易对
WEBSOCKETS_SEND_SUB_SET = set()

# ws 枚举值
CLIENT = WsType.CLIENT.value
SPIDER = WsType.SPIDER.value
STORE = WsType.STORE.value

SUB = WsType.SUB.value
UNSUB = WsType.UNSUB.value
PUB = WsType.PUB.value

ws_host_ping_map = {
    'host': None
}

async def ws_start(websocket, path):
    """
    功能:
        websocket 处理
        1. 消息来源分两部分: spider 和 客户端
        2. spider 收订阅, 发trade
        3. 客户端 发订阅, 收trade
        4. store 做调度 和数据保存
    消息格式:

        client>sub.trades/btcusdt:huobipro # 客户端发订阅
        client>unsub.trades/btcusdt:huobipro # 客户端取消订阅

        spider>pub.trades/btcusdt:huobiproO[[]] # spider 发布

        store>sub.trades/btcusdt:huobipro   # store 发订阅
        store>unsub.trades/btcusdt:huobipro   # store  取消订阅

        store>pub.trades/btcusdt:huobiproO[[]] # store 发布
    """
    global WEBSOCKETS_USER_SUB_MAP
    global WEBSOCKETS_SEND_SUB_SET
    ws_key = websocket.remote_address
    try:
        if websocket.messages.empty():
            await ping(websocket)
        async for message in websocket:
            await ping(websocket)
            if len(message) < 100:
                print(f"{now()} > {message}")
            if CLIENT in message:
                if '/' in message:
                    a, sub_type, b = message.replace('>', '.').split('.')
                    symbol_ex_id = message.split('/')[-1]
                    if sub_type == SUB:
                        WEBSOCKETS_SEND_SUB_SET.add(message.replace(CLIENT, STORE))
                        if symbol_ex_id not in WEBSOCKETS_USER_SUB_MAP['trades']:
                            WEBSOCKETS_USER_SUB_MAP['trades'][symbol_ex_id] = {ws_key: websocket}
                        else:
                            WEBSOCKETS_USER_SUB_MAP['trades'][symbol_ex_id].update({ws_key: websocket})
                    elif sub_type == UNSUB:
                        if symbol_ex_id in WEBSOCKETS_USER_SUB_MAP['trades']:
                            try:
                                WEBSOCKETS_USER_SUB_MAP['trades'][symbol_ex_id].pop(ws_key)
                            except:
                                ...
            elif SPIDER in message:
                # 缓存 spider ws
                SPIDER_WEBSOCKETS_MAP[ws_key] = websocket

                if 'O' in message:
                    _, data = message.split('O')
                    symbol_ex_id = _.split('/')[-1]
                    symbol, exchange_id = symbol_ex_id.split(':')
                    save_data_to_db(symbol, exchange_id, data)
                    if symbol_ex_id in WEBSOCKETS_USER_SUB_MAP['trades']:
                        for key, ws in WEBSOCKETS_USER_SUB_MAP['trades'][symbol_ex_id].items():
                            await ping(ws)
                            if not ws.closed:
                                await ws.send(message.replace(SPIDER, STORE))
                            else:
                                # 移除掉 已关闭的 ws
                                WEBSOCKETS_USER_SUB_MAP['trades'][symbol_ex_id].pop(key)
            # 发送订阅 到 spider
            if WEBSOCKETS_SEND_SUB_SET:
                sub_info = WEBSOCKETS_SEND_SUB_SET.pop()
                for key, ws in SPIDER_WEBSOCKETS_MAP.items():
                    await ping(ws)
                    if not ws.closed:
                        await ws.send(sub_info)
                    else:
                        SPIDER_WEBSOCKETS_MAP.pop(key)
    except ConnectionClosed:
        ...

def now():
    """
    功能:
        格式化 当前时间
    """
    return datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")

async def send_message(websocket, message):
    """
    功能:
        发送消息
    """
    print(f"{now()} < {message}")
    await websocket.send(message)

def save_data_to_db(symbol, exchange_id, data):
    """
    功能:
        把消息存储到本地
    """
    key = settings.EXCHANGE_SYMBOL_TRADE_KEY.format(exchange_id, symbol)
    REDIS_CON.rpush(key, *eval(data))

async def ping(websocket):
    global ws_host_ping_map
    is_ping = False
    now = int(time.time())
    host = websocket.remote_address
    if host not in ws_host_ping_map:
        is_ping = True
    elif (now - ws_host_ping_map[host]) > settings.PING_PONG_SECONDS:
        is_ping = True
    if is_ping:
        await websocket.ping()
        await websocket.send(f'{STORE}>ping: {now}')
        ws_host_ping_map[host] = now


class Command(BaseCommand):

    """
    CMD:
        python manage.py run_ws_server
    说明:
        开启 存储 ws 服务
    """

    def handle(self, *args, **options):
        print('Start Ws Server...')
        try:
            args_list = [
                (ws_start, storekeeper_ws_conf["host"], storekeeper_ws_conf["port"]),
            ]
            tasks = [asyncio.ensure_future(websockets.serve(*x)) for x in args_list]
            asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            exit(0)
