#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 16:13
import os
import sys

if __name__ == "__main__":
    arguments = sys.argv
    ENV = 'dev'
    if 'prod' in arguments:
        ENV = 'prod'
        arguments.remove('prod')
    elif 'dev' in arguments:
        arguments.remove('dev')
    elif 'storekeeper' in arguments:
        ENV = 'storekeeper'
        arguments.remove('storekeeper')
    elif 'spider_kline' in arguments:
        ENV = 'spider_kline'
        arguments.remove('spider_kline')
    elif 'storekeeper_dev' in arguments:
        ENV = 'storekeeper_dev'
        arguments.remove('storekeeper_dev')
    elif 'storekeeper_test' in arguments:
        ENV = 'storekeeper_test'
        arguments.remove('storekeeper_test')
    elif 'storekeeper_prod' in arguments:
        ENV = 'storekeeper_prod'
        arguments.remove('storekeeper_prod')
    elif 'local_dev' in arguments:
        ENV = 'local_dev'
        arguments.remove('local_dev')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', f'exspider.settings.{ENV}')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)