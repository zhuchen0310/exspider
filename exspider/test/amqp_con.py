#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-12 18:18
# coding=utf-8
import asyncio
import aio_pika
import logging
from aiologger.loggers.json import JsonLogger

logger = JsonLogger.with_default_handlers(level=logging.DEBUG,
                                          serializer_kwargs={'indent': 2},
                                          name='my-test',
                                          formatter=logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] '
                                                                      '[%(levelname)s] [%(threadName)s] %(message)s'),
                                          loop=asyncio.get_event_loop())


class AioPikaConsumer(object):
    def __init__(self, url, loop, msg_process_func, queue_name, routing_keys='market.*.*.*.*.*.*.*', exchange_name='market'):
        self._url = url  # "amqp://guest:guest@192.168.11.145/"
        self._loop = loop
        self._msg_process_func = msg_process_func
        self._queue_name = queue_name
        self._routing_keys = routing_keys  # ["market.ticker.exhange.base.quote.market_type.market_subtype.incr", ...]
        self._exchange_name = exchange_name
        self._connection = None
        self._channel = None

    def on_connection_reconnect(self, *args):
        print("on_connection_reconnect: %s" % str(*args))
        self._loop.create_task(self.consuming())

    def on_connection_close(self, *args):
        print("on_connection_close: %s" % str(*args))
        # await self._connection.reconnect()
        # self._connection.add_timeout(5, self.reconnect)

    async def consuming(self):
        async with self._connection:
            # Creating channel
            self._channel = await self._connection.channel()
            # Declaring exchange
            exchange = await self._channel.declare_exchange(self._exchange_name, type=aio_pika.ExchangeType.TOPIC,
                                                      auto_delete=False)
            # Declaring queue
            queue = await self._channel.declare_queue(self._queue_name, auto_delete=False, arguments={'x-max-length': 3000})
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
                            await logger.error("Exception to process message: %s from routing_key: %s. %s" % (
                                message.body.decode(), message.routing_key, e))

    async def start(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(self._url, loop=self._loop)
                self._connection.add_close_callback(self.on_connection_close)
                self._connection.add_reconnect_callback(self.on_connection_reconnect)
                await self.consuming()
            except Exception as e:
                await logger.error("Exception to consuming message, %s" % e)


class AioPikaPublisher(object):
    def __init__(self, url, loop, exchange_name='market'):
        self._url = url  # "amqp://guest:guest@192.168.11.145/"
        self._loop = loop
        self._exchange_name = exchange_name
        self._connection = None
        self._channel = None
        self._exchange = None

    async def connect(self):
        if not self._exchange:
            try:
                self._connection = await aio_pika.connect(self._url, loop=self._loop)
                self._channel = await self._connection.channel()  # type: aio_pika.Channel
                self._exchange = await self._channel.declare_exchange(self._exchange_name, type=aio_pika.ExchangeType.TOPIC,
                                                                      auto_delete=False)
                await logger.info("RabbitMQ is connected.")
                # await self._exchange.publish(aio_pika.Message(body="hello,xxxx".encode()),
                #                              routing_key="market.ticker.OKEX.BTC_USDT.SPOT.0.0")
            except Exception as e:
                await logger.error('Failed to connect to RabbitMQ. Exception: %s.' % e)

    async def close(self):
        if self._connection:
            await self._connection.close()
            self._exchange = None

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
        except Exception as e:
            await logger.error('Failed to publish data to rabbitMQ. Exception: %s, %s.' % (type(e), e))
            await logger.error('Failed to publish to rabbitMQ, routing_key[%s], data: %s' % (routing_key, data))
            # 重新连接RabbitMQ Server
            self._exchange = None
            await self.connect()

