#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import time

import aiohttp
import gzip

url = 'wss://api.huobi.pro/ws'
proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()


tms = int(time.time())

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                "sub": f"market.btcusdt.kline.1min",
                "id": tms
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
                    print('>', gzip.decompress(msg.data).decode())
                    # print(json.dumps(json.loads(msg.data), indent=2))


if __name__ == '__main__':
    loop.run_until_complete(go())