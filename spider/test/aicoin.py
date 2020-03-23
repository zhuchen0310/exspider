#!/usr/bin/env python
  
import asyncio
import aiohttp
import websockets
import datetime
import re
from collections import deque
  
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
    print(f"{now()} > {message}")
    if 'O[[' in message:
        ...
        # re_trade = re.findall(r'(\[{2}.+?\]{2})', message)
        # trade_list = []
        # for x in re_trade:
        #     trade_list.extend(eval(x))
        # if trade_list:
        #     await get_ohlcv_by_trade(trade_list)
 
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
            message = (await websocket.receive()).data
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
  
async def get_ohlcv_by_trade(trade_list):
    """
    功能:
        根据 trade 生成 1min k线
    """
 
    global ohlcv
    for trade in trade_list:
        price = trade[3]
        volume = trade[4]
        tms = trade[0] - trade[0] % 60 # 1分钟
        new_ohlcv = [tms, price, price, price, price, volume]
        if not ohlcv:
            ohlcv = deque([new_ohlcv], maxlen=2)
        else:
            # update
            last_ohlcv = ohlcv[-1]
            last_tms = last_ohlcv[0]
            if tms == last_tms:
                if price > last_ohlcv[2]:
                    last_ohlcv[2] = price
                if price < last_ohlcv[3]:
                    last_ohlcv[3] = price
                last_ohlcv[4] = price
                last_ohlcv[5] += volume
                ohlcv[-1] = last_ohlcv
            else:
                ohlcv.append(new_ohlcv)
    print(ohlcv)
      
  
asyncio.get_event_loop().run_until_complete(handler())