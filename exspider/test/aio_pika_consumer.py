# -*- coding:utf-8 -*-
import sys
import getopt
import asyncio
import logging
from aiologger.loggers.json import JsonLogger
from amqp_con import AioPikaConsumer

logger = JsonLogger.with_default_handlers(level=logging.DEBUG,
                                          serializer_kwargs={'indent': 2},
                                          name='my-test',
                                          formatter=logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] '
                                                                      '[%(levelname)s] [%(threadName)s] %(message)s'),
                                          loop=asyncio.get_event_loop())


async def msg_process_func(routing_key, data):
    routing_key_words = routing_key.split(".")
    symbol = routing_key_words[3] + routing_key_words[4]
    if 'btcusdt' == symbol:
        await logger.debug("routing_key: %s, data: %s" % (routing_key, data))


if __name__ == "__main__":
    opts = []
    args = []
    try:
        # opts 获取参数元组(-m file_address)，args 获取剩余非（-arg para）格式的参数
        opts, args = getopt.getopt(sys.argv[1:], "l:k:q:",
                                   ["mq_url=", "routing_keys=", "queue_name="])
    except getopt.GetoptError:
        print('Error args.')
        exit(-1)
    mq_url = "amqp://guest:guest@149.129.90.193:5672/" # "amqp://guest:guest@192.168.11.145/"
    routing_keys = "market.kline.huobipro.*.*.*.*.*"
    queue_name = 'test001'
    for name, value in opts:
        if name in ("-l", "--mq_url"):
            mq_url = value
        elif name in ("-k", "--routing_keys"):
            routing_keys = value
        elif name in ("-q", "--queue_name"):
            queue_name = value
    print("mq_url=%s, routing_key=%s, queue_name=%s." % (mq_url, routing_keys, queue_name))
    loop = asyncio.get_event_loop()
    consumer = AioPikaConsumer(mq_url, loop, msg_process_func=msg_process_func,
                               queue_name=queue_name, routing_keys=routing_keys)
    loop.run_until_complete(consumer.start())
    loop.close()

