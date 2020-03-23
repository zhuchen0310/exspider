#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-09 10:19

import ujson
import asyncio

from django.core.management import BaseCommand
from exspider.utils.redis_con import AsyncRedis
from aicoin_pair_json import pair_map

loop = asyncio.get_event_loop()

class Command(BaseCommand):

    """
    CMD:
        python manage.py init_aicoin_pair
    """

    def handle(self, *args, **options):
        redis_con = AsyncRedis(loop)
        map_dict = {
            f'{x["s"].replace("_", "")}:{x["eid"]}':
                ujson.dumps(x)
            for x in pair_map
        }
        loop.run_until_complete(redis_con.init_aicoin_pair_cache(map_dict))

