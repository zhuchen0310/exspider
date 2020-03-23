#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-29 14:14
import asyncio
from django.core.management import BaseCommand
from django.db import connection
from spider import ws_crawl
import ccxt

loop = asyncio.get_event_loop()
proxy = 'http://127.0.0.1:6152'
proxies = {
	'http': 'http://127.0.0.1:6152',
	'https': 'http://127.0.0.1:6152'
}


class Command(BaseCommand):
	cursor = connection.cursor()

	def handle(self, *args, **options):
		ws_exchanges = ws_crawl.exchanges
		ccxt_exchanges = ccxt.exchanges

		# for ws_exchange_id in ws_exchanges:
		# 	self.handle_ws_exchange(ws_exchange_id)
		for ccxt_exchange_id in ccxt_exchanges:
			if ccxt_exchange_id not in ws_exchanges:
				self.handle_ccxt_exchange(ccxt_exchange_id)

	def handle_ws_exchange(self, exchange_id):
		s_sql = f'select * from exchange_pairs where exchange_id="{exchange_id}" LIMIT 1;'
		print(s_sql)
		if self.cursor.execute(s_sql):
			print(exchange_id, '已经存在!')
			return
		try:
			ex = getattr(ws_crawl, exchange_id)(loop, http_proxy=proxy)
			values = [(ex.exchange_id, x.upper()) for x in ex.symbols]
			self.cursor.executemany("""insert into exchange_pairs (exchange_id, pair) values (%s, %s);""", values)
		except:
			print(f'{exchange_id} 失败!')

	def handle_ccxt_exchange(self, exchange_id):
		s_sql = f'select * from exchange_pairs where exchange_id="{exchange_id}" LIMIT 1;'
		if self.cursor.execute(s_sql):
			print(exchange_id, '已经存在!')
			return
		try:
			ex = getattr(ccxt, exchange_id)()
			ex.proxies = proxies
			ex.load_markets()
			values = [(ex.id, x.replace('/', '')) for x in ex.symbols]
			self.cursor.executemany("""insert into exchange_pairs (exchange_id, pair) values (%s, %s);""", values)
		except Exception as e:
			print(f'{exchange_id} 失败!')