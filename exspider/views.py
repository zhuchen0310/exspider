#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-06 15:13
import datetime

import requests
import time

from django.conf import settings
from rest_framework.views import APIView
from exspider.utils.view_funcs import (
    http_response
)
from exspider.utils.socket_server import send_notice_to_php
from storekeeper.push_center import push_noon_broadcast


class PushMessage(APIView):


    def get(self, request):
        """
        返回某个交易所的交易对
        """
        exchange_id = request.query_params.get('exchange_id', '')
        symbol = request.query_params.get('symbol', '')
        message = request.query_params.get('message', '')
        base_name, quote_name = symbol.split('_')
        data = self.go(exchange_id, base_name, quote_name, message)
        return http_response(1, http_msg='success', data=data)

    def go(self, exchange_id, base_name, quote_name, message):
        exchange_data_api = f'{settings.C21_CANDLES_API}/pair/infos?exchange_id={exchange_id}&base_name={base_name}&quote_name={quote_name}'
        try:
            data = requests.get(exchange_data_api).json()['data']
        except:
            data = {}
        ret_data = {
            'pair_id': data.get('pair_id', ''),
            'coin_id': data.get('coin_id', ''),
            'has_ohlcv': data.get('has_ohlcv', '')
        }
        title = f'{base_name.upper()}价格异动'
        if not message:
            message = '【短时下跌】据币安行情，BTC过去4分钟下跌1.04%，下跌金额为73.45USDT，现报7096.0。5月17日19:43'
        push_data = {
            'uids': [],
            'title': title,
            'coin_id': ret_data['coin_id'],
            'body': message,
            'summary': message,
            'jump': settings.PAIR_JUMP_URL.format(
                symbol=f'{base_name}-{quote_name}',
                exchange_id=exchange_id,
                pair_id=ret_data['pair_id'],
                hasOhlcv=ret_data['has_ohlcv'],
                coin_id=ret_data['coin_id'],
            ),
            'timestamp': int(time.time()),
            'type': 'PairAlarm',
            'message_push_type': 'remind',
        }
        send_notice_to_php(settings.PHP_NOTICE_METHOD['pair_alarm'], data=push_data)
        print(title, message)
        return {
            'title': title,
            'message': message
        }


class PushNoonMessage(APIView):

    def get(self, request):
        """
        返回某个交易所的交易对
        """
        tms = request.query_params.get('tms', None)
        today_tms = int(time.mktime(datetime.datetime.now().date().timetuple()))
        if 11.8 * 60 * 60 <= int(tms) - today_tms <= 12.3 * 60 * 60:
            return http_response(9001, http_msg='error')
        ret = push_noon_broadcast()
        return http_response(1, http_msg='success' if ret else 'push_failed')