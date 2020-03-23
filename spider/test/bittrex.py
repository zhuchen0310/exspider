#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51

import asyncio
import time
from base64 import b64decode
from zlib import decompress, MAX_WBITS
import requests

import aiohttp
import json
from urllib.parse import urlencode



def get_ws_url():
    connectionData = '[{"name":"c2"}]'
    clientProtocol = '1.5'
    transport = 'webSockets'
    tid = '1'

    data = requests.get('https://socket.bittrex.com/signalr/negotiate', params={"clientProtocol": clientProtocol,"connectionData": connectionData}).json()
    print(data)

    connectionToken = data['ConnectionToken']

    req_data = {
        'connectionToken': connectionToken,
        'connectionData': connectionData,
        'tid': tid,
        'transport': transport,
        'clientProtocol': clientProtocol,
    }
    params = urlencode(req_data)
    url = f'wss://socket.bittrex.com/signalr/connect?{params}'
    return url

url = get_ws_url()
# proxy = 'http://127.0.0.1:6152'
proxy = None
loop = asyncio.get_event_loop()

tms = int(time.time())

async def go():
    global tms
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {
                'H': 'c2',
                'M': 'QueryExchangeState',
                'A': ['USD-BTC'],
                'I': 0
            }
            print('<', sub_data)
            await ws.send_json(sub_data)
            while 1:
                try:
                    msg = await ws.receive()
                    ret = json.loads(msg.data)
                    # print(ret)
                    # if 'M' in ret and ret['M']:
                    #     en_data = ret['M'][0]['A'][0]
                    if ret.get('R'):
                        en_data = ret['R']
                        try:
                            deflated_msg = decompress(
                                b64decode(en_data, validate=True), -MAX_WBITS)
                        except SyntaxError:
                            deflated_msg = decompress(b64decode(en_data, validate=True))
                        data = json.loads(deflated_msg)
                        print('>', data['M'])
                        print(data['f'][0])
                        sub_data = {
                            'H': 'c2',
                            # 'M': 'SubscribeToSummaryDeltas',
                            # 'M': 'SubscribeToSummaryLiteDeltas',
                            'M': 'QueryExchangeState',
                            'A': (),
                            'I': 0
                        }
                        time.sleep(1)
                        await ws.send_json(sub_data)
                except:
                    continue


if __name__ == '__main__':
    loop.run_until_complete(go())