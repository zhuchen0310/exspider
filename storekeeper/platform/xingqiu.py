#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-20 12:07
import time
import ujson

from django.conf import settings

from exspider.utils.enum_con import PushType
from .base import PlatformBase


class xingqiu(PlatformBase):

    def __init__(self):
        super().__init__()
        self.platform_name = 'xingqiu'
        self.token = '5114762c2bc135e6dd2905dd16b3dd89'
        self.test_api = 'https://test01service.odaily.com'
        self.prod_api = 'https://service.odaily.com'
        if settings.APP_TYPE == 'WWW':
            self.push_url = f'{self.prod_api}/api/newsflash-push'
        else:
            self.push_url = f'{self.test_api}/api/newsflash-push'
        self.push_type = {
            PushType.rise_or_fall.value: 1,
            PushType.pass_price.value: 2,
            PushType.noon_broadcast.value: 3
        }
        self.push_params = {
            'token': self.token,
            'push_type': '',
            'push_content': {},
        }
        self.alarm_info = {
            # 'huobipro': {
            #     'name': '火币',
            #     'alarm_coin': ['btc', 'eth'],
            #     'alarm_price_pass': {'btc': 100, 'eth': 50},
            #     # 'price_pass_cache': {},  # 同一价格 7天内只push 一次
            #     'noon_broadcast_pairs': {
            #         'btcusdt': 'BTC/USDT',
            #         'ethusdt': 'ETH/USDT',
            #         'xrpusdt': 'XRP/USDT',
            #         'eosusdt': 'EOS/USDT',
            #         'bchusdt': 'BCH/USDT',
            #         'ltcusdt': 'LTC/USDT',
            #         'bsvusdt': 'BSV/USDT',
            #     },
            # },
            'binance': {
                'name': '币安',
                # 'alarm_coin': ['btc', 'eth'],
                'alarm_coin': [],
                'alarm_price_pass': {'btc': 500, 'eth': 50},
                'price_pass_cache': {},  # 整数关口缓存
                'pass_cache_ex_tms': 10 * 60,  # 整数关口的过期时间: 10分钟
                'noon_broadcast_pairs': {
                    'btcusdt': 'BTC/USDT',
                    'ethusdt': 'ETH/USDT',
                    'xrpusdt': 'XRP/USDT',
                    'eosusdt': 'EOS/USDT',
                    'bchabcusdt': 'BCH/USDT',
                    'ltcusdt': 'LTC/USDT',
                },
            }
        }


    async def http_push(self, push_data):
        # push_data = {'token': '5114762c2bc135e6dd2905dd16b3dd89', 'push_type': 3, 'push_content': '{"exchange":"\\u706b\\u5e01","undulation":[{"transactions":"BTC\\/USDT","change":"\\u4e0a\\u6da8","percent":"0.7%","current_price":"10888.75 USDT"},{"transactions":"ETH\\/USDT","change":"\\u4e0b\\u8dcc","percent":"1.37%","current_price":"309.48 USDT"},{"transactions":"XRP\\/USDT","change":"\\u4e0b\\u8dcc","percent":"2.0%","current_price":"0.46530000000000005 USDT"},{"transactions":"EOS\\/USDT","change":"\\u4e0b\\u8dcc","percent":"2.6%","current_price":"7.1972 USDT"},{"transactions":"BCH\\/USDT","change":"\\u4e0b\\u8dcc","percent":"2.24%","current_price":"474.72 USDT"},{"transactions":"LTC\\/USDT","change":"\\u4e0b\\u8dcc","percent":"3.77%","current_price":"135.98 USDT"},{"transactions":"BSV\\/USDT","change":"\\u4e0b\\u8dcc","percent":"1.1%","current_price":"237.7803 USDT"}]}'}
        response = await self.http.get_json_data(self.push_url, request_method='POST', data=push_data)
        await self.logger.info(response)

    async def get_push_price_rise_fall_data(self, base_name, exchange_id, data):
        symbol = base_name.upper()
        now_price = data['now_price']
        minutes_ago = data['minutes_ago']
        is_rise = data['is_rise']
        pct = data['pct']
        rise_str = '上涨' if is_rise else '下跌'
        exchange = self.alarm_info[exchange_id]['name']
        rate = await self.get_usdt_2_cny_rate()
        cny_price = format(float(now_price) * rate, '.2f')
        data = self.push_params
        data['push_type'] = self.push_type[PushType.rise_or_fall.value]
        data['push_content'] = ujson.dumps({
            'exchange': exchange,
            'currency': symbol,
            'change': rise_str,
            'percent': f'{pct}%',
            'current_price': f'{now_price} USDT(￥{cny_price})',
            'time': minutes_ago,
        })
        return data

    async def get_push_price_pass_data(self, base_name, exchange_id, data):
        symbol = base_name.upper()
        now_price = data['now_price']
        pass_price = int(data['pass_price'])
        pct = data['pct']
        rise_str_2 = data['rise_str_2']
        if pass_price % self.alarm_info[exchange_id]['alarm_price_pass'][base_name]:
            return
        if pass_price not in self.alarm_info[exchange_id]['price_pass_cache']:
            self.alarm_info[exchange_id]['price_pass_cache'][pass_price] = int(time.time())
        else:
            return
        is_rise = data['is_rise']
        rise_str = '突破' if is_rise else '跌破'
        exchange = self.alarm_info[exchange_id]['name']

        data = self.push_params
        data['push_type'] = self.push_type[PushType.pass_price.value]
        data['push_content'] = ujson.dumps({
            'exchange': exchange,
            'change': rise_str_2,
            'percent': f'{pct}%',
            'currency': symbol,
            'current_price': f'{now_price} USDT',
            'incident': [f'{rise_str}{pass_price} USDT']
        })
        return data

    async def get_push_noon_broadcast_data(self, push_type, exchange_id, data_list):
        data = self.push_params
        data['push_type'] = self.push_type[push_type]
        undulation = []
        exchange = self.alarm_info[exchange_id]['name']
        for d in data_list:
            now_price = d['now_price']
            pair_name = d['pair_name']
            change = '上涨' if d['is_rise'] else '下跌'
            percent = f"{d['pct']}%"
            undulation.append({
                'transactions': pair_name,
                'change': change,
                'percent': percent,
                'current_price': f'{now_price} USDT'
            })
        data['push_content'] = ujson.dumps({
            'exchange': exchange,
            'undulation': undulation
        })
        return data
