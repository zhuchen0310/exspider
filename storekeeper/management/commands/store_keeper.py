# -*- coding:utf-8 -*-
import time
import asyncio
from django.core.management import BaseCommand
from storekeeper.store_data import StoreData
from exspider.common.funcs import get_spider_host_map
from exspider.utils.amqp_con import ConsumError
from exspider.utils.email_con import send_email
from django.conf import settings


class Command(BaseCommand):
    """
    CMD:
        python manage.py store_keeper 10001 spider2 market.*.*.*.*.*.*
    """
    def add_arguments(self, parser):
        parser.add_argument('server_id', type=str)
        parser.add_argument('spider_name', type=str)
        parser.add_argument('routing_key', type=str, default='market.*.*.*.*.*.*')

    def handle(self, *args, **options):
        print('Start...')
        server_id = options['server_id']
        spider_name = options['spider_name']
        routing_key = options['routing_key']
        host = get_spider_host_map()[spider_name]
        mq_url = f'amqp://guest:guest@{host}:5672/'
        if spider_name == 'global':
            mq_url = f'amqp://hold:JustHold1t@{host}:5672//'
        loop = asyncio.get_event_loop()
        try:
            store_data = StoreData(server_id, mq_url, routing_key, loop)
            loop.run_until_complete(store_data.start())
        except ConsumError:
            if settings.APP_TYPE == 'WWW':
                send_email(settings.RECIVER_EMAIL_LIST, title='MQ 消费者断开', body=f'{spider_name}|{server_id}|{routing_key} 无法消费!')
            time.sleep(10)
            print('Stop...')
            exit(0)


