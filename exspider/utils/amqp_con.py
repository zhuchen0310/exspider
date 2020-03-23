#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-12 18:18
# coding=utf-8
import aio_pika
from aio_pika.pool import Pool

from exspider.utils.logger_con import get_logger

logger = get_logger(__name__)


GLOBAL_CHANNEL_POOL_MAP = {}


class ConsumerError(BaseException):
    """
    消费者连接错误
    """
    ...

class ConsumError(BaseException):
    """
    消费错误
    """
    ...

class PublisherError(BaseException):
    """
    发布者连接错误
    """
    ...

class PublishError(BaseException):
    """
    发布错误
    """
    ...


class AioPikaPool:

    def __init__(self, url, loop, *args, **kwargs):
        self._url = url  # "amqp://guest:guest@192.168.11.145/"
        self._loop = loop
        self._channel_pool = None
        self._max_pool_size = 2
        self._max_channel_pool_size = 50

    async def get_channel_pool(self):
        url = self._url
        loop = self._loop

        async def get_connection():
            return await aio_pika.connect_robust(url, loop=loop)

        async def get_channel() -> aio_pika.Channel:
            async with connection_pool.acquire() as connection:
                return await connection.channel()

        if url not in GLOBAL_CHANNEL_POOL_MAP:
            connection_pool = Pool(get_connection, max_size=self._max_pool_size, loop=loop)
            channel_pool = Pool(get_channel, max_size=self._max_channel_pool_size, loop=loop)
            GLOBAL_CHANNEL_POOL_MAP[url] = channel_pool
        else:
            channel_pool = GLOBAL_CHANNEL_POOL_MAP[url]
        self._channel_pool = channel_pool
        return channel_pool

class AioPikaConsumer(AioPikaPool):
    def __init__(self, url, loop, msg_process_func, queue_name, routing_keys='market.*.*.*.*.*.*.*', exchange_name='market'):
        super().__init__(url, loop)
        # self._url = url  # "amqp://guest:guest@192.168.11.145/"
        # self._loop = loop
        self._msg_process_func = msg_process_func
        self._queue_name = queue_name
        self._routing_keys = routing_keys  # ["market.ticker.exhange.base.quote.market_type.market_subtype.incr", ...]
        self._exchange_name = exchange_name

    def on_connection_reconnect(self, *args):
        print("on_connection_reconnect: %s" % str(*args))
        self._loop.create_task(self.consuming())

    def on_connection_close(self, *args):
        print("on_connection_close: %s" % str(*args))
        # await self._connection.reconnect()
        # self._connection.add_timeout(5, self.reconnect)

    async def consuming(self):
        if not self._channel_pool:
            await self.get_channel_pool()
        async with self._channel_pool.acquire() as channel:
            await channel.set_qos()

            exchange = await channel.declare_exchange(self._exchange_name, type=aio_pika.ExchangeType.TOPIC,
                                                      auto_delete=False)
            # Declaring queue
            queue = await channel.declare_queue(self._queue_name, auto_delete=False,
                                                arguments={'x-max-length': 3000})
            # Binding queue
            if isinstance(self._routing_keys, list):
                for routing_key in self._routing_keys:
                    await queue.bind(exchange, routing_key)
            else:
                await queue.bind(exchange, self._routing_keys)
            await logger.info("aio pika consumer declare queue: %s, bind %s, and start to process message." % (
                self._queue_name, self._routing_keys))
            async with queue.iterator() as queue_iter:
                # Cancel consuming after __aexit__
                async for message in queue_iter:
                    async with message.process():
                        try:
                            # print(message.body.decode())
                            await self._msg_process_func(message.routing_key, message.body.decode())
                            # await self._channel.close()
                        except Exception as e:
                            raise ConsumerError("Exception to process message: %s from routing_key: %s. %s" % (
                                message.body.decode(), message.routing_key, e))
    async def start(self):
        while True:
            try:
                await self.consuming()
            except ConsumerError as e:
                raise ConsumError(e)
            except Exception as e:
                await logger.error(f"Exception to consuming message, {e}")
                raise ConsumError(e)


class AioPikaPublisher(AioPikaPool):
    def __init__(self, url, loop, exchange_name='market'):
        super().__init__(url, loop)
        self._exchange_name = exchange_name
        self._exchange = None

    async def connect(self):
        if not self._exchange:
            await self.get_channel_pool()
            try:
                async with self._channel_pool.acquire() as channel:
                    await channel.set_qos()
                    self._exchange = await channel.declare_exchange(self._exchange_name, type=aio_pika.ExchangeType.TOPIC,
                                                                          auto_delete=False)
                await logger.info("RabbitMQ is connected.")
            except Exception as e:
                raise PublisherError(f'Failed to connect to RabbitMQ. Exception: {e}.')

    async def publish(self, routing_key, data):
        """向RabbitMQ发送数据"""
        try:
            if self._exchange:
                await self._exchange.publish(aio_pika.Message(body=data.encode()), routing_key=routing_key)
            else:
                await logger.error(u'Data publish failed，please connect RabbitMQ first.')
                # 重新连接RabbitMQ Server
                self._exchange = None
                await self.connect()
        except PublisherError as e:
            raise PublishError(e)
        except Exception as e:
            await logger.error('Failed to publish data to rabbitMQ. Exception: %s, %s.' % (type(e), e))
            await logger.error('Failed to publish to rabbitMQ, routing_key[%s], data: %s' % (routing_key, data))
            # 重新连接RabbitMQ Server
            self._exchange = None
            await self.connect()

