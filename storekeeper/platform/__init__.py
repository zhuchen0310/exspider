#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-06-20 12:15
"""
第三方平台接口
"""
from .xingqiu import xingqiu
from .test_push import test_push




platforms = [
    'xingqiu',
    # 'test_push',
]




__all__ = ('platforms')