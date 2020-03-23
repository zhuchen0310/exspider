#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 11:14
"""Tcp Server客户端,超时2s"""
import ujson
import time
from socket import *
from django.conf import settings
import logging
logger = logging.getLogger(settings.APP_NAME)

class TcpClient:
    def __init__(self, host, port, bufsiz):
        self.host = host
        self.port = port
        self.bufsiz = bufsiz
        self.addr = (host, port)
        self.tcpClientSock = None  # socket close之后,不可再使用

    def reconnect(self):
        """PHP的server最大socket连接数为1000"""
        self.tcpClientSock = socket(AF_INET, SOCK_STREAM)
        self.tcpClientSock.settimeout(2)  # 设置超时
        self.tcpClientSock.connect(self.addr)

    def send_data(self, data):
        self.tcpClientSock.send(ujson.dumps(data).encode())
        data = self.tcpClientSock.recv(self.bufsiz)
        return data.decode()

    def close(self):
        self.tcpClientSock.close()

tcp_client = TcpClient(
        settings.PHP_NOTICE_CONNECT['HOST'],
        settings.PHP_NOTICE_CONNECT['PORT'],
        settings.PHP_NOTICE_CONNECT['BUFSIZE'])
try:
    tcp_client.reconnect()
except Exception as e:
    logger.error(f'socket_server_error: {e}')

def send_notice_to_php(call_func, data=None, datas=None):
    """
    功能:
        发送通知到PHP, datas参数用于批量传输参数, list类型
    """
    # time.sleep(20)
    # 聚合发送数据
    ret = False
    send_datas = []
    if data:
        data = {"param": data}
        data.update(call_func)
        send_datas.append(data)
    if datas:
        for d in datas:
            data = {"param": d}
            data.update(call_func)
        send_datas.append(data)
    # send data
    try:
        logger.info(f'通知数据为: {send_datas}')
        for send_data in send_datas:
            r_data = tcp_client.send_data(send_data)
            if r_data:
                logger.info(f'返回: {r_data}')
        ret = True
    except Exception as e:
        tcp_client.reconnect()
        # print('通知数据为:', send_datas)
        for send_data in send_datas:
            r_data = tcp_client.send_data(send_data)
            if r_data:
                logger.info(f'返回: {r_data}')
        ret = True
        print('error:', str(e))
    finally:
        return ret

# test
if __name__ == "__main__":
    from django.conf import settings



    tcp_client.connect()

    while True:
        data = input('>')
        if not data:
            break
        param = {
            "user_id": 47,
            "coin_id": "301",
            "timestamp": int(round(time.time() * 1000))
        }
        data = {
            "param": param
        }
        data.update(settings.PHP_NOTICE_METHOD['coin_follow'])
        print(data)
        try:
            r_data = tcp_client.send_data(data)
        except Exception:  # 断线重连
            tcp_client.connect()
            r_data = tcp_client.send_data(data)
        if not r_data:
            break
        print(r_data.strip())

    tcp_client.close()
