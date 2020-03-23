#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-10 16:09

import time
import datetime
import requests
import logging

from django.conf import settings

from exspider.utils.socket_server import send_notice_to_php

logger = logging.getLogger(__name__)
push_logger = logging.getLogger(settings.APP_NAME)


class WhaleAlert:

    def __init__(self):
        self.uri = 'https://api.whale-alert.io/v1/'
        self.api_key = '1h9RKrVso8o2LKWjyzXg5MKgzsaI0Yar'
        self.start_tms = None
        self.min_value = 5000000
        self.time_out = 20
        self.apis = {
            'status': f'status?api_key={self.api_key}',
            'transactions': f'transactions?api_key={self.api_key}&start={{}}&end={{}}&min_value={self.min_value}&limit=100'
        }
        self.symbol_map = {}
        self.alarm_min_usd = settings.WHALE_ALERT_USD_AMOUNT

    def market_loads(self):
        response = requests.get(f'{self.uri}{self.apis["status"]}', timeout=self.time_out)
        if response.status_code == 200:
            data = response.json()
            blockchains = data.get('blockchains', None)
            if not blockchains:
                logger.error('blockchains is empty!')
                return
            for block_chain in blockchains:
                if block_chain.get('status', '') != 'connected':
                    logger.error(f'status is not connected: {block_chain}')
                    continue
                block_chain_name = block_chain['name']
                self.symbol_map.update({x: block_chain_name for x in block_chain['symbols']})

    def fetch_alert(self):
        now = int(time.time())
        if not self.start_tms:
            start_tms = now - 60 * 60 + 6
        else:
            start_tms = self.start_tms
        url = f'{self.uri}{self.apis["transactions"].format(start_tms, now)}'
        logger.info(f'url: {url}')
        response = requests.get(url, timeout=self.time_out)
        data = response.json()
        if 'error' in data.get('result'):
            logger.error(f'error: {data}')
            if '3600 seconds' in data.get('message'):
                self.start_tms = None
            return
        if 'success' in data.get('result') and data.get('count', 0):
            transactions = data.get('transactions', None)
            if not transactions:
                return
            self.start_tms = transactions[-1]['timestamp']
            for transaction in transactions:
                timestamp = transaction['timestamp']
                amount = int(transaction['amount'])
                origin_amount_usd = transaction['amount_usd']
                amount_usd = f'{int(origin_amount_usd / 10000)}万' if origin_amount_usd < 100000000 else f'{int(origin_amount_usd / 100000000)}亿'
                symbol = transaction['symbol'].upper()
                if symbol not in settings.WHALE_ALERT_TOKEN_MAP:
                    continue
                from_type = transaction['from']['owner_type']
                from_address = transaction['from']['address']
                is_from_exchange = False
                is_to_exchange = False
                if from_type == 'exchange':
                    from_exchange = transaction['from']['owner']
                    if 'unknown' in from_exchange:
                        is_from_exchange = False
                        from_exchange = '未知'
                    else:
                        is_from_exchange = True
                else:
                    from_exchange = '未知'
                to_type = transaction['to']['owner_type']
                to_address = transaction['to']['address']
                if to_type == 'exchange':
                    to_exchange = transaction['to']['owner']
                    if 'unknown' in to_exchange:
                        is_to_exchange = False
                        to_exchange = '未知'
                    else:
                        is_to_exchange = True
                else:
                    to_exchange = '未知'
                if to_address == 'Multiple Addresses':
                    to_address = ''
                if from_address == 'Multiple Addresses':
                    from_address = ''

                # huobi 交易所多个地址
                # (huobi 交易所地址)
                # 未知地址
                # 多个未知地址
                if is_from_exchange and from_address:
                    from_exchange_str = f'{from_exchange}交易所地址'
                elif is_from_exchange and not from_address:
                    from_exchange_str = f'{from_exchange}交易所多个地址'
                elif not is_from_exchange and not from_address:
                    from_exchange_str = f'多个{from_exchange}地址'
                else:
                    from_exchange_str = '未知地址'

                if is_to_exchange and to_address:
                    to_exchange_str = f'{to_exchange}交易所地址'
                elif is_to_exchange and not to_address:
                    to_exchange_str = f'{to_exchange}交易所多个地址'
                elif not is_to_exchange and not to_address:
                    to_exchange_str = f'多个{to_exchange}地址'
                else:
                    to_exchange_str = '未知地址'

                from_str = f'{from_exchange_str}' if not from_address else f'{from_address}（{from_exchange_str}）'
                to_str = f'{to_exchange_str}' if not to_address else f'{to_address}（{to_exchange_str}）'

                if is_from_exchange or is_to_exchange:
                    logger.info(f'据Whale Alert监测：{timestamp_2_str(timestamp)}，{amount}个{symbol}（价值{amount_usd}美元）从{from_str} 转至 {to_str}。')
                if origin_amount_usd >= self.alarm_min_usd:
                    title = f'{symbol}大额转账监控'
                    if not is_from_exchange and not is_to_exchange:
                        continue
                    if from_exchange == to_exchange:
                        continue
                    sub_title = f'据监测，{symbol}于{timestamp_2_str(timestamp, is_day_str=False)}出现大额转账：{amount}个{symbol}（价值{amount_usd}美元）从{from_exchange_str}转至{to_exchange_str}。'
                    message = f'据Whale Alert数据显示：{timestamp_2_str(timestamp)}，{amount}个{symbol}（价值{amount_usd}美元）从{from_str} 转至 {to_str}。'
                    push_logger.info(f'{message}')
                    coin_id = settings.WHALE_ALERT_TOKEN_MAP.get(symbol)
                    if not coin_id:
                        continue
                    data = {
                        'uids': [],
                        'coin_id': coin_id,
                        'title': title,
                        'summary': sub_title,
                        'body': message,
                        'timestamp': int(time.time()),
                        'type': 'PairAlarm',
                        'is_push': 1,
                        'message_push_type': 'information',
                    }
                    send_notice_to_php(settings.PHP_NOTICE_METHOD['pair_alarm'], data=data)

    def start(self):
        # self.market_loads()
        while True:
            logger.info(f'Start... {datetime.datetime.now()}')
            try:
                self.fetch_alert()
            except Exception as e:
                logger.error(e)
            time.sleep(self.time_out)

def timestamp_2_str(timestamp, format='%H:%M', is_day_str=True):
    """
    功能:
        时间戳 转换成固定字符串
    实例:
        timestamp: 1557987600
        return: 5月16日14:20
    """
    struct_time = time.localtime(timestamp)
    if is_day_str:
        time_str = f"{struct_time.tm_mon}月{struct_time.tm_mday}日{time.strftime('%H:%M', struct_time)}"
    else:
        time_str = time.strftime(format, struct_time)
    return time_str

if __name__ == '__main__':
    whale_alert = WhaleAlert()
    whale_alert.start()
