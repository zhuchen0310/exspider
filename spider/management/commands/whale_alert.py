#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-11 13:05

from spider.spiders.whale_alert import WhaleAlert
from django.core.management import BaseCommand


class Command(BaseCommand):

    """
    CMD:
        python manage.py whale_alert
    """

    def handle(self, *args, **options):
        whale_alert = WhaleAlert()
        whale_alert.start()