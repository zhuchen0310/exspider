#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import base64
import gzip
import time

import aiohttp
import json
import zlib


url = 'wss://ws-feed.pro.coinbase.com'
proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()

tms = int(time.time())

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                "type": "subscribe",
                "product_ids": [
                    "BTC-USD",
                ],
                "channels": [
                    "ticker",
                    "heartbeat",
                    # {
                    #     "name": "full",
                    #     "product_ids": [
                    #         # "ETH-BTC",
                    #         # "ETH-USD"
                    #     ]
                    # }
                ]
            }
            print('<', sub_data)
            await ws.send_json(sub_data)
            while 1:
                try:
                    msg = await ws.receive()
                except:
                    continue
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    res = json.loads(msg.data)
                    try:
                        if res.get('type') == 'ticker' and res.get('trade_id'):
                            trades = [
                                [
                                    res['time'],
                                    res['trade_id'],
                                    res['side'],
                                    res['price'],
                                    res['last_size'],
                                ]
                            ]
                            print('>', trades)
                    except:
                        print(res)
                    # print(res)
                    # if "ping" in res:
                    #     print(res)
                    #     pong = msg.data.replace("ping", "pong")
                    #     await ws.send_str(pong)
                    #     return
                    # if not res or not isinstance(res, list):
                    #     return
                    # if not res[0].get('data', None):
                    #     return
                    # en_data = res[0]['data']
                    # de_data = base64.b64decode(en_data)
                    # trades = gzip.decompress(de_data).decode('utf-8')

if __name__ == '__main__':
    loop.run_until_complete(go())