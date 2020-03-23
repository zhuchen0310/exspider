#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-04-01 15:14
import sys
from line_profiler import LineProfiler
import asyncio


class Prof(LineProfiler):
    """
    对函数进行性能分析
    from exspider import prof

    @prof
    def func():
        pass

    示例1:
    with prof:
        func()
    示例2:
    async with prof:
        func()
    """

    def __enter__(self):
        self.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()  # 停止性能分析
        self.print_stats(sys.stdout)

    async def __aenter__(self):
        await asyncio.coroutine(self.enable)()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.coroutine(self.disable)()
        await asyncio.coroutine(self.print_stats())(sys.stdout)


prof = Prof()