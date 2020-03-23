#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import time

import aiohttp
import json
import zlib


url = 'wss://api.bitfinex.com/ws'
proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()


def get_ping_data(cid):
    return {"event":"ping", "cid": cid}

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                "event": "subscribe",
                "channel": "trades",
                "pair": "BTCUSD"
            }
            print('<', sub_data)
            await ws.send_json(sub_data)
            while 1:
                try:
                    msg = await ws.receive()
                except:
                    continue
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    # print('>', (msg.data))
                    ret = json.loads(msg.data)
                    if 'hb' in ret:
                        await ws.send_json(get_ping_data(ret[0]))
                    if 'tu' in ret:
                        [
                            1,
                            "tu",
                            "13370954-BTCUSD",
                            344993241,
                            1553507002,
                            4045.3,
                            -0.07462766
                        ]
                        trades = [[ret[4], ret[3], 'sell' if ret[6] < 0 else 'buy', ret[5], abs(ret[6])]]
                        print(trades)
                    print(json.dumps(json.loads(msg.data), indent=2))


if __name__ == '__main__':
    loop.run_until_complete(go())