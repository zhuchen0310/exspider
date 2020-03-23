#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-12 16:26
from __future__ import absolute_import

import os
import logging
import sys

from celery import Celery, platforms
from django.conf import settings

logger = logging.getLogger(__name__)

platforms.C_FORCE_ROOT = True  # 可以在root 下执行
arguments = sys.argv
ENV = 'dev'
if 'test' in arguments:
    ENV = 'test'
    arguments.remove('test')
elif 'prod' in arguments:
    ENV = 'prod'
    arguments.remove('prod')
elif 'dev' in arguments:
    arguments.remove('dev')
elif 'local_dev' in arguments:
    ENV = 'local_dev'
    arguments.remove('local_dev')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', f'exspider.settings.{ENV}')
app = Celery('exspider')
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

if __name__ == '__main__':
    app.start()
