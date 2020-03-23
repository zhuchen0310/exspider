#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-20 12:07
import time

from django.conf import settings

from exspider.utils.enum_con import PushType
from .base import PlatformBase
from exspider.utils.email_con import Email
email = Email()

RECIVER_LIST = [
    'xinfeng@ihold.com',
    'zhuchen@ihold.com'
]

class test_push(PlatformBase):

    def __init__(self):
        super().__init__()
        self.platform_name = 'test_push'
        self.token = '13109-3257444f1dead9f223afa036438f5ff0'
        self.test_api = 'https://pushbear.ftqq.com/sub'
        self.prod_api = 'https://pushbear.ftqq.com/sub'
        if settings.APP_TYPE == 'WWW':
            self.push_url = f'{self.prod_api}'
        else:
            self.push_url = f'{self.test_api}'
        self.push_type = {
            PushType.rise_or_fall.value: 1,
            PushType.pass_price.value: 2,
            PushType.noon_broadcast.value: 3
        }
        self.push_params = {
            'sendkey': self.token,
            'text': '',
            'desp': {},
        }
        self.alarm_info = {
            'huobipro': {
                'alarm_coin': ['uuu'],
                'alarm_price_pass': {'btc': 100, 'eth': 50},
                'price_pass_cache': {},  # 整数关口缓存
                'pass_cache_ex_tms': 10 * 60,  # 整数关口的过期时间: 10分钟
                'noon_broadcast_pairs': {
                    # 'btcusdt': 'BTC/USDT',
                    # 'uuuBTC': 'UUU/BTC',
                }
            },
        }


    async def http_push(self, push_data):
        await self.logger.info(f'{self.platform_name}: {push_data}')
        if settings.APP_TYPE != 'WWW':
            await email.async_send_email(reciver=RECIVER_LIST, title=push_data['text'], body=push_data['desp'])

    async def get_push_price_rise_fall_data(self, base_name, exchange_id, data):
        symbol = base_name.upper()
        now_price = data['now_price']
        minutes_ago = data['minutes_ago']
        is_rise = data['is_rise']
        pct = data['pct']
        rise_str = '上涨' if is_rise else '下跌'
        exchange = '火币'
        data = self.push_params
        data['text'] = f'{symbol}{rise_str}{pct}%'
        data['desp'] = f'{exchange}: {symbol}过去{minutes_ago}分钟{rise_str}{pct}%; 当前价: {now_price}'
        return data

    async def get_push_price_pass_data(self, base_name, exchange_id, data):
        symbol = base_name.upper()
        now_price = data['now_price']
        pass_price = int(data['pass_price'])
        if pass_price % self.alarm_info[exchange_id]['alarm_price_pass'][base_name]:
            return
        is_rise = data['is_rise']
        rise_str = '突破' if is_rise else '跌破'
        exchange = '火币'

        data = self.push_params
        data['text'] = f'{symbol}{rise_str}{pass_price}'
        data['desp'] = f'{exchange}: {symbol}{rise_str}: {pass_price}, 现报: {now_price}'
        return data
