#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 17:59
from __future__ import absolute_import

from django.core.management import BaseCommand
from django.conf import settings
from exspider.utils import prometheus_funcs

from prometheus_client import start_http_server, Summary
import time

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')


# Decorate function with metric.
@REQUEST_TIME.time()
def process_request(t):
    """A dummy function that takes some time."""
    prometheus_funcs.exchange_status()
    prometheus_funcs.exchange_spider_status()
    prometheus_funcs.exchange_spider_symbol_status()
    time.sleep(t)



class Command(BaseCommand):

    """
    CMD:
        python manage.py start_prometheus_server
    """

    def handle(self, *args, **options):
        print('Start prometheus server...')
        # Start up the server to expose the metrics.
        start_http_server(8001)
        # Generate some requests.
        while True:
            try:
                process_request(settings.CARE_INTERVAL_TIME)
            except KeyboardInterrupt:
                break
            except:
                continue
