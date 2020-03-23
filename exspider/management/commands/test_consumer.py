#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-19 12:02

import asyncio

from django.core.management import BaseCommand
from django.conf import settings

from exspider.utils.amqp_con import AioPikaConsumer

async def msg_process_func(routing_key, data):
    print("routing_key: %s, data: %s" % (routing_key, data))


class Command(BaseCommand):

    def handle(self, *args, **options):
        loop = asyncio.get_event_loop()
        consumer = AioPikaConsumer(settings.RABBIT_MQ_URL, loop,
                                   msg_process_func=msg_process_func, queue_name='test_server_001')
        loop.run_until_complete(consumer.start())
        loop.close()