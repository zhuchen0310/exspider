#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-20 15:24
import re

from django.core.management import BaseCommand
from exspider.utils.pg_con import TradePg

import asyncio

loop = asyncio.get_event_loop()

import aiohttp
import datetime

MESSAGE_SEPERATOR = chr(30)
MESSAGE_PART_SEPERATOR = chr(31)

ohlcv = None


def now():
    return datetime.datetime.now().strftime("%Y%m%d %H:%M:%S")


async def send_message(websocket, message):
    print(f"{now()} < {message}")
    await websocket.send_str(message.replace(' ', MESSAGE_PART_SEPERATOR))


async def consumer(websocket, message):
    message = message.strip(MESSAGE_SEPERATOR).replace(MESSAGE_PART_SEPERATOR, ' ')
    # print(f"{now()} > {message}")
    if 'O[[' in message:
        re_trade = re.findall(r'(\[{2}.+?\]{2})', message)
        trade_list = []
        for x in re_trade:
            x = x.replace('ask', 's').replace('bid', 'b')
            trade_list.extend(eval(x))
        if trade_list:
            async with TradePg('aicoin_trade') as pg:
                await pg.insert_many('btcusdt', trade_list)

    # C PI
    # C PO
    if message.startswith('C PI'):
        await send_message(websocket, 'C PO')


async def handler():
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=conn) as session:
        async with session.ws_connect('wss://wsv3.aicoin.net.cn/deepstream',
                                      autoclose=True,
                                      autoping=True,
                                      ) as websocket:
            # C CH
            # C CHR wss://wsv3.aicoin.net.cn/deepstream
            # C A
            async for msg in websocket:
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    message = msg.data
                    await consumer(websocket, message)
                    await send_message(websocket, 'C CHR wss://wsv3.aicoin.net.cn/deepstream')
                    message = (await websocket.receive()).data
                    await consumer(websocket, message)

                    # A REQ {"username":"visitor","password":"password"}
                    # A A L
                    await send_message(websocket, 'A REQ {"username":"visitor","password":"password"}')
                    message = (await websocket.receive()).data
                    await consumer(websocket, message)

                    # await send_message(websocket, 'E S trades/btcusdt:binance')
                    await send_message(websocket, 'E S trades/btcusdt:huobipro')

                    async for message in websocket:
                        await consumer(websocket, message.data)
                elif msg.type in [aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED]:
                    return

async def main():
    while 1:
        try:
            await handler()
        except Exception as e:
            print(e)
            continue

class Command(BaseCommand):
    def handle(self, *args, **options):
        asyncio.get_event_loop().run_until_complete(main())
