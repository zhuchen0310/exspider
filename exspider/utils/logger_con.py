#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-03-07 16:12

import asyncio
import logging
import platform
import sys
from logging.handlers import RotatingFileHandler

class AsyncLog:

    def __init__(self, name, level=logging.DEBUG, loop=None):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self._loop = None
        self._loop = loop if loop else self.loop
        self._dummy_task = None

    @property
    def loop(self):
        if self._loop is not None and self._loop.is_running():
            return self._loop
        self._loop = asyncio.get_event_loop()
        return self._loop

    def __make_dummy_task(self):
        async def _dummy(*args, **kwargs):
            return

        return self.loop.create_task(_dummy())

    def _make_log_task(self, level, msg, *args, **kwargs):
        """
        Creates an asyncio.Task for a msg if logging is enabled for level.
        Returns a dummy task otherwise.
        """
        if not self.logger.isEnabledFor(level):
            if self._dummy_task is None:
                self._dummy_task = self.__make_dummy_task()
            return self._dummy_task

        if kwargs.get("exc_info", False):
            if not isinstance(kwargs["exc_info"], BaseException):
                kwargs["exc_info"] = sys.exc_info()

        coro = self._log(  # type: ignore
            level, msg, *args, caller=self.logger.findCaller(False), **kwargs
        )
        return self.loop.create_task(coro)

    async def _log(
        self,
        level,
        msg,
        args,
        exc_info=None,
        extra=None,
        stack_info=False,
        caller = None,
    ):
        sinfo = None
        if logging._srcfile and caller is None:  # type: ignore
            # IronPython doesn't track Python frames, so findCaller raises an
            # exception on some versions of IronPython. We trap it here so that
            # IronPython can use logging.
            try:
                fn, lno, func, sinfo = self.logger.findCaller(stack_info)
            except ValueError:  # pragma: no cover
                fn, lno, func = "(unknown file)", 0, "(unknown function)"
        elif caller:
            fn, lno, func, sinfo = caller
        else:  # pragma: no cover
            fn, lno, func = "(unknown file)", 0, "(unknown function)"
        if exc_info and isinstance(exc_info, BaseException):
            exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
        record = logging.LogRecord(  # type: ignore
            name=self.name,
            level=level,
            pathname=fn,
            lineno=lno,
            msg=msg,
            args=args,
            exc_info=exc_info,
            func=func,
            sinfo=sinfo,
            extra=extra,
        )
        return self.logger.handle(record)

    async def debug(self, msg, *args, **kwargs):
        return self._make_log_task(logging.DEBUG, msg, args, **kwargs)

    async def info(self, msg, *args, **kwargs):
        return self._make_log_task(logging.INFO, msg, args, **kwargs)

    async def warning(self, msg, *args, **kwargs):
        return self._make_log_task(logging.WARNING, msg, args, **kwargs)

    async def error(self, msg, *args, **kwargs):
        return self._make_log_task(logging.ERROR, msg, args, **kwargs)



class MyLog(AsyncLog):

    def __init__(self, name, level=logging.DEBUG):
        super().__init__(name, level)
        self.logger = self.setup_custom_logger(name, level)

    @staticmethod
    def setup_custom_logger(name, log_level):
        formatter = logging.Formatter(fmt='%(asctime)s %(filename)s[line:%(lineno)d] '
                                          '[%(levelname)s] [%(threadName)s] %(message)s')
        logger = logging.getLogger(name)
        logger.setLevel(log_level)

        file_handler = RotatingFileHandler('server.log', mode='a', maxBytes=10 * 1024 * 1024, backupCount=20)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger


def get_logger(name=__name__, is_debug=True, is_async=True):
    """
    功能:
        获取 logger 实例
    :param name:
    :param is_debug:
    :param is_async: 是否获取 异步logger
    :return:
    用法:
        1. 普通函数logger
            logger.info('23333')
        2. 协程logger
            await logger.info('23333')
    """
    if not is_async:
        logger = logging.getLogger(name)
        logger.setLevel(level=logging.DEBUG if is_debug else logging.INFO)
        return logger
    if "Windows" == platform.system():
        logger = MyLog(name, level=logging.DEBUG if is_debug else logging.INFO)
    else:
        logger = AsyncLog(name, level=logging.DEBUG if is_debug else logging.INFO)
    return logger

