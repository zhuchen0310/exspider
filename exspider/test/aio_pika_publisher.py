import asyncio
from exspider.utils.amqp_con import AioPikaPublisher


async def main(loop):
    # routing_key规则：market.ticker.exhange.symbol.market_type.market_subtype.incr
    routing_key = "market.kline.okex.btcusdt.SPOT.0.0"
    publisher = AioPikaPublisher("amqp://guest:guest@192.168.11.145/", loop)
    await publisher.connect()
    print("go to publish message.")
    for i in range(0, 3):
        print("go to publish message %s." % i)
        await publisher.publish(routing_key, 'Hello, world %s!' % i)
    await publisher.close()

    # connection = await aio_pika.connect_robust(
    #     "amqp://guest:guest@192.168.11.145/", loop=loop)
    #
    # async with connection:
    #     # routing_key = "test_queue"
    #
    #     channel = await connection.channel()
    #     exchange = await channel.declare_exchange("market", type=aio_pika.ExchangeType.TOPIC, auto_delete=False)
    #
    #     await exchange.publish(
    #         aio_pika.Message(
    #             body='Hello world!'.encode()
    #         ),
    #         routing_key=routing_key
    #     )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    print("loop run complete.")
    loop.close()

