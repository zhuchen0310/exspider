#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-06 15:18
import time

from django.conf import settings
from rest_framework.response import Response



# 成功
success_code = {
    1: "{}"
}

# 用户错误
user_error_code = {
    1001: "登录失败",
    1002: "重复请求验证码",
    1003: "验证码错误",
    1004: "您已经登录",
    1005: "需要登录才能操作",
    1006: "验证码过期",
    1007: "稍后再试",
    1008: "{}"
}

# 系统错误
http_error_code = {
    9001: "必传参数[{}]错误",
    9002: "[{}]参数错误",
    9003: "[{}]格式错误",
    9004: "自定义错误",
    9005: "数据不存在",
    9006: "数据添加失败,{}",
    9007: "数据保存失败",
    9008: "{}"  # 自定义错误，客户端展示
}


def http_response(http_code, http_msg=None, data=None, **kwargs):
    resp = settings.RESPONSE_FORMAT.copy()
    resp['code'] = http_code
    if http_code in user_error_code:
        resp['message'] = user_error_code[http_code]
    elif http_code in http_error_code:
        resp['message'] = http_error_code[http_code]
    else:
        resp['message'] = success_code[http_code]
    if http_msg is not None:
        resp['message'] = resp['message'].format(http_msg)
    if data is not None:
        resp['data'] = data
    resp['server_time'] = int(time.time())
    resp.update(kwargs)
    return Response(resp)