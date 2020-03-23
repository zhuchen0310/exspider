#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import time
import requests
import aiohttp
import json

proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()

def get_ping():
    return {
          "id": int(time.time()),
          "type":"ping"
        }


url = 'wss://ws.bitstamp.net'


tms = int(time.time())

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                "event": "bts:subscribe",
                "data": {
                    "channel": 'live_trades_btcusd'
                },
            }
            print('<', sub_data)
            await ws.send_json(sub_data)
            while 1:
                try:
                    msg = await ws.receive(timeout=0.01)
                except:
                    if time.time() - tms > 50:
                        tms = time.time()
                        await ws.send_json(get_ping())
                    continue
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    data = json.loads(msg.data)
                    print(data)
                    if not data or not data.get('data') or not data.get('channel'):
                        continue
                    symbol = data['channel'].split('_')[-1].lower()
                    trade_data = data['data']
                    trades = [
                                [
                                    trade_data['timestamp'],
                                    f'{trade_data["id"]}',
                                    'buy' if int(trade_data['type']) == 0 else 'sell',
                                    float(trade_data['price']),
                                    float(trade_data['amount']),
                                ]
                            ]
                    print('>', symbol, trades)
                else:
                    print(msg)
                    # try:
                    #     if res.get('type') == 'ticker' and res.get('trade_id'):
                    #         trades = [
                    #             [
                    #                 res['time'],
                    #                 res['trade_id'],
                    #                 res['side'],
                    #                 res['price'],
                    #                 res['last_size'],
                    #             ]
                    #         ]
                    #         print('>', trades)
                    # except:
                    #     print(res)
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