#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-19 15:51
import asyncio

from django.test import TestCase

# from .hotbit import Hotbit as Exchange
# from .mxc import mxc as Exchange
# from .idax import idax as Exchange
# from .biki import biki as Exchange
# from .biss import biss as Exchange
from .zb import zb as Exchange
# from .gateio import gateio as Exchange
# from .hitbtc import hitbtc as Exchange
# from .kraken import kraken as Exchange

loop = asyncio.get_event_loop()
# proxy = 'http://127.0.0.1:1087'
proxy = 'http://127.0.0.1:6152'


class HotbitTests(TestCase):
    """
    说明:
        1. 在单个交易所中编辑 相应方法
        2. 导入交易所
        3. 执行测试命令: python manage.py test spider.test.test_exchange local_dev -k
    """

    @classmethod
    def setUpTestData(cls):
        cls.ws = Exchange(proxy=proxy)

    def test_restful(self):
        ret = loop.run_until_complete(self.ws.parse_restful_kline())
        print('Start test RESTful...')
        print('*' * 20)
        print(ret)
        print('*' * 20)
        print()
        # [t, o, h, l, c, v]
        self.assertIsInstance(ret, list)
        self.assertEqual(len([x for x in ret if x]), 6)
        self.assertEqual(max([float(x) for x in ret[1:-1]]), float(ret[2]))
        self.assertEqual(min([float(x) for x in ret[1:-1]]), float(ret[3]))

    def test_websocket(self):
        print()
        print('Start test websocket...')
        self.ws.ping_interval_seconds = 5
        loop.run_until_complete(self.ws.add_sub_data(self.ws.sub_data))
        self.assertNotEqual(len(self.ws._pending_sub_data), 0)
        loop.run_until_complete(self.ws.get_ws_data_forever(self.ws.ws_url))

