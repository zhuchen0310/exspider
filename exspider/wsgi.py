#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-04 16:28

"""
WSGI config for exspider project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.0/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "exspider.settings")

application = get_wsgi_application()
