#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import base64
import gzip
import time
import requests
import aiohttp
import json
import zlib
proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()

ws_id = f'{int(time.time())}'

def get_ws_url():
    proxies = {'http': 'http://127.0.0.1:6152', 'https': 'http://127.0.0.1:6152'}
    data = requests.post('https://api.kucoin.com/api/v1/bullet-public', proxies=proxies).json()
    if not data or not data.get('data'):
        raise BaseException('request error')
    token = data['data']['token']
    ws_api = data['data']['instanceServers'][0]['endpoint']
    url = f'{ws_api}?token={token}&connectId={ws_id}'
    return url

def get_ping():
    return {
          "id": int(time.time()),
          "type":"ping"
        }


url = get_ws_url()



tms = int(time.time())

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                "id": ws_id,
                "type": "subscribe",
                "topic": "/market/match:BTC-USDT",
                "privateChannel": False,
                "response": True
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
                    res = json.loads(msg.data)
                    print(res)
                    if res and res.get('data'):
                        data = res.get('data')
                        symbol = data['symbol']
                        trades = [
                                    [
                                        int(data['time']) // (10 ** 9),
                                        f'{data["sequence"]}',
                                        data['side'],
                                        float(data['price']),
                                        float(data['size']),
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