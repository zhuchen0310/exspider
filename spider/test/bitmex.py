import asyncio
import aiohttp
import json

proxy = 'http://127.0.0.1:6152'
loop = asyncio.get_event_loop()
url = 'wss://www.bitmex.com/realtime'

async def start():
    conn = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(loop=loop, connector=conn) as session:
        async with session.ws_connect(url, proxy=proxy) as ws:
            sub_data = {"op": "subscribe", "args": ["trade:XBTUSD"]}
            await ws.send_json(sub_data)
            async for msg in ws:
                if msg.type in [aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY]:
                    print('>')
                    print(json.dumps(json.loads(msg.data), indent=2))


if __name__ == "__main__":
    loop.run_until_complete(start())