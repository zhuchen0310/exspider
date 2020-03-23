#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-20 12:07
import ujson

from django.conf import settings

from exspider.utils.http_base import HttpBase
from exspider.utils.enum_con import PushType
from exspider.utils.redis_con import AsyncRedisBase
from exspider.utils.logger_con import get_logger

ENCODING = 'utf-8'

class PlatformBase:

    def __init__(self):
        self.logger = get_logger(__name__)
        self.push_logger = get_logger(settings.APP_NAME)
        self.platform_name = ''
        self.http = HttpBase()
        self._redis = AsyncRedisBase()
        self._cur = None
        self.token = ''
        self.test_api = ''
        self.prod_api = ''
        if settings.APP_TYPE == 'WWW':
            self.push_url = f'{self.prod_api}/api/newsflash-push'
        else:
            self.push_url = f'{self.test_api}/api/newsflash-push'
        self.http.headers = {
            'Content-Type': "application/x-www-form-urlencoded",
        }
        self.push_type = {
            PushType.rise_or_fall.value: 1,
            PushType.pass_price.value: 2,
            PushType.noon_broadcast.value: 3
        }
        # 请求参数
        self.push_params = {
            'token': self.token,
            'push_type': '',
            'push_content': {},
        }
        # 配置信息
        self.alarm_info = {
            'huobipro': {
                'name': '火币',
                'alarm_coin': ['btc', 'eth'],
                'alarm_price_pass': {'btc': 500, 'eth': 100},
                'price_pass_cache': {},  # 同一价格 10分钟内只push 一次
                'pass_cache_ex_tms': 10 * 60, # 整数关口的过期时间: 10分钟
                'noon_broadcast_pairs': {
                    'btcusdt': 'BTC/USDT',
                    'ethusdt': 'ETH/USDT',
                    'xrpusdt': 'XRP/USDT',
                    'eosusdt': 'EOS/USDT',
                    'bchusdt': 'BCH/USDT',
                    'ltcusdt': 'LTC/USDT',
                    'bsvusdt': 'BSV/USDT',
                },
            },
        }

    @property
    async def init_redis(self):
        if not self._cur or self._cur.closed:
            self._cur = await self._redis.redis
        return

    async def push_message_to_platform(self, push_type, base_name, exchange_id, data):
        """
        功能:
            push 消息到 第三方: 星球日报
        """
        if push_type == PushType.rise_or_fall.value:
            push_data = await self.get_push_price_rise_fall_data(base_name, exchange_id, data)
        elif push_type == PushType.pass_price.value:
            push_data = await self.get_push_price_pass_data(base_name, exchange_id, data)
        elif push_type == PushType.noon_broadcast.value:
            push_data = await self.get_push_noon_broadcast_data(push_type, exchange_id, data)
        else:
            raise BaseException('push_type is error!')
        if not push_data:
            return
        await self.push_logger.info(f'{self.platform_name}: {push_data}')
        await self.http_push(push_data)
        return True

    async def http_push(self, push_data):
        """
        功能:
            平台 push 接口 每个平台复写
        """
        ...

    async def get_push_price_rise_fall_data(self, base_name, exchange_id, data):
        """
        功能:
            获取 价格异动 push 数据
        """
        ...

    async def get_push_price_pass_data(self, base_name, exchange_id, data):
        """
        功能:
            获取 整数关口 push 数据
        """
        ...

    async def get_push_noon_broadcast_data(self, push_type, exchange_id, data_list):
        """
        功能:
            获取 午间播报 push 数据
        """
        ...

    async def get_usdt_2_cny_rate(self):
        """
        功能:
            获取 usdt >> cny 的汇率
        """
        await self.init_redis
        usdt_cny_data = await self._cur.hget('token_price', f'1753-2088', encoding=ENCODING)
        rate = ujson.loads(usdt_cny_data)['price']
        return rate
