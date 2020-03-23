#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-08 14:58

import hashlib
import asyncio
import time

import aiosmtplib

from django.conf import settings

from smtplib import SMTP_SSL
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

USER = settings.EMAIL_CONF['user']
PW = settings.EMAIL_CONF['pw']
SENDER = settings.EMAIL_CONF['user']
SMTPSVR = settings.EMAIL_CONF['SMTPSVR']
PORT = settings.EMAIL_CONF['PORT']

EMAIL_CACHE = {}    # 已发送队列, 同一消息 每5分钟发一次


class Email:

    def __init__(self, loop=None, timeout=1):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.timeout = timeout # 超时时间

    async def async_send_email(self, reciver, title, body, sender=SENDER, exchange_id='', symbol=''):
        ret = True
        smtp = aiosmtplib.SMTP()
        if not await self.check_is_send_this_message(body):
            return False
        try:
            msg = MIMEMultipart()
            msg['From'] = '%s <%s>' % (Header("noreply", "utf-8"), sender)
            if isinstance(reciver, list):
                msg['To'] = ';'.join(reciver)
            else:
                msg['To'] = reciver
            # msg['To'] = reciver
            msg['Subject'] = Header(title, 'utf-8')
            msg['Accept-Language'] = 'zh-CN'
            msg["Accept-Charset"] = "ISO-8859-1,utf-8"
            body = MIMEText(body, 'html', 'utf-8')
            body.set_charset("utf-8")
            msg.attach(body)

            await smtp.connect(hostname=SMTPSVR, port=PORT, use_tls=True)
            await smtp.login(USER, PW)
            await smtp.send_message(msg)
        except Exception as e:
            print(f"send email: {e}")
            ret = False
        finally:
            smtp.close()
        return ret

    async def check_is_send_this_message(self, body):
        global EMAIL_CACHE
        msg_hash = hashlib.md5(body.encode()).hexdigest()
        now = int(time.time())
        pop_msg_map = {
            x: EMAIL_CACHE[x]
            for x in EMAIL_CACHE if now - EMAIL_CACHE[x] <= 5 * 60
        }
        EMAIL_CACHE = pop_msg_map
        if msg_hash not in EMAIL_CACHE:
            EMAIL_CACHE[msg_hash] = now
            return True
        return False


def send_email(reciver, title, body, sender=SENDER, exchange_id='', symbol=''):
    loop = asyncio.get_event_loop()
    email = Email(loop)
    ret = loop.run_until_complete(email.async_send_email(reciver, title, body, sender=sender, exchange_id=exchange_id, symbol=symbol))
    return ret