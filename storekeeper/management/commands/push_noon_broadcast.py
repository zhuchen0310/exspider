#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-20 17:07

from django.core.management import BaseCommand
from storekeeper.push_center import push_noon_broadcast
class Command(BaseCommand):

    """
    CMD:
        python manage.py push_noon_broadcast
    """

    def handle(self, *args, **options):
        push_noon_broadcast()
