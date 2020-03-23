#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import time

import aiohttp
import json
import zlib


url = 'wss://real.okex.com:10442/ws/v3'
proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

tms = int(time.time())

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                "op": "subscribe",
                "args": ["spot/trade:BTC-USDT", "spot/trade:ETH-USDT"]
            }
            print('<', sub_data)
            await ws.send_json(sub_data)
            while 1:
                if int(time.time()) - tms > 30:
                    tms = int(time.time())
                    await ws.send_json('ping')
                try:
                    msg = await ws.receive()
                except:
                    continue
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    print('>', inflate(msg.data))
                    # print(json.dumps(json.loads(msg.data), indent=2))


if __name__ == '__main__':
    loop.run_until_complete(go())