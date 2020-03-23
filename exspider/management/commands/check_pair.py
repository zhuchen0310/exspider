#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-29 15:32
from django.core.management import BaseCommand
from django.db import connection

not_active_pair_ids = []


class Command(BaseCommand):
    def handle(self, *args, **options):
        cursor = connection.cursor()
        exchange_sql = 'SELECT DISTINCT(exchange_id) from exchange_pairs;'
        exchange_pair_sql = 'select pair from exchange_pairs where exchange_id="{}";'
        pair_runtime_sql = 'select symbol, pair_name, id from pair_runtime where exchange="{}";'
        cursor.execute(exchange_sql)
        all_exchange_ids = [x[0] for x in cursor.fetchall()]
        for exchange_id in all_exchange_ids:
            cursor.execute(exchange_pair_sql.format(exchange_id))
            active_pairs = [x[0] for x in cursor.fetchall()]
            cursor.execute(pair_runtime_sql.format(exchange_id))
            pair_runtime_dict = {
                f'{x[0]}{x[1]}': x[2]
                for x in cursor.fetchall()
            }
            for pair in pair_runtime_dict:
                if pair not in active_pairs:
                    not_active_pair_ids.append(f'{exchange_id},{pair_runtime_dict[pair]},{pair}\n')
        print(len(not_active_pair_ids))
        with open('./pair.txt', 'w') as f:
            f.writelines(not_active_pair_ids)

