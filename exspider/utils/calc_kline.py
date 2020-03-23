# -*- coding:utf-8 -*-
import datetime
import copy
import time
import calendar
from exspider.utils.enum_con import TimeFrame

# 周期对应的分钟数值
PERIOD_INTERVAL_MAP = {
    TimeFrame.m1.value: 1,
    TimeFrame.m3.value: 3,
    TimeFrame.m5.value: 5,
    TimeFrame.m15.value: 15,
    TimeFrame.m30.value: 30,
    TimeFrame.h1.value: 60,
    TimeFrame.h2.value: 120,
    TimeFrame.h3.value: 180,
    TimeFrame.h4.value: 240,
    TimeFrame.h6.value: 360,
    TimeFrame.h12.value: 720,
    TimeFrame.d1.value: 1440,
    TimeFrame.d2.value: 2880,
    TimeFrame.d3.value: 4320,
    TimeFrame.d5.value: 7200,
    TimeFrame.w1.value: 10080,
    TimeFrame.M1.value: 43200,
    TimeFrame.Y1.value: 518400
}


def get_week_monday(ts_sec):
    """获取时间戳ts_sec（秒值）所在周的周一日期"""
    monday = datetime.datetime.fromtimestamp(ts_sec)
    one_day = datetime.timedelta(days=1)
    while monday.weekday() != 0:
        monday -= one_day
    monday_zero = monday - datetime.timedelta(hours=monday.hour, minutes=monday.minute, seconds=monday.second,
                                              microseconds=monday.microsecond) + datetime.timedelta(hours=8)
    this_monday_ts = time.mktime(monday_zero.timetuple())
    return this_monday_ts


def get_month_first_next_first(ts_sec):
    """获取时间戳ts_sec（秒值）所在月的月初日期和下个月的月初日期"""
    dt = datetime.datetime.fromtimestamp(ts_sec)
    first_day = datetime.datetime(dt.year, dt.month, 1, 8)
    days_num = calendar.monthrange(dt.year, dt.month)[1]
    first_day_of_next_month = first_day + datetime.timedelta(days=days_num)
    return time.mktime(first_day.timetuple()), time.mktime(first_day_of_next_month.timetuple())


def calc_kline(base_bar_list, step, skip_header, skip_tail=False):
    """根据输入的K线数据，计算其它周期的K线
    skip_header：是否跳过头部数据的计算
    """
    if len(base_bar_list) < 2:
        return base_bar_list
    step_interval_sec = PERIOD_INTERVAL_MAP[step] * 60
    base_start = 0  # 从时间戳对齐step的位置开始计算
    if skip_header:
        if TimeFrame.w1.value == step:
            for base_start in range(0, len(base_bar_list)):
                if base_bar_list[base_start][0] == get_week_monday(base_bar_list[base_start][0]):
                    break  # 从周一开始
        if TimeFrame.M1.value == step:
            for base_start in range(0, len(base_bar_list)):
                first_day, first_day_of_next_month = get_month_first_next_first(base_bar_list[base_start][0])
                if base_bar_list[base_start][0] == first_day:
                    break  # 从月初开始
        else:
            for base_start in range(0, len(base_bar_list)):
                if 0 == base_bar_list[base_start][0] % step_interval_sec:
                    break
    base_end = len(base_bar_list) - 1  # 从时间戳对齐step的位置结束计算
    if skip_tail:
        if TimeFrame.w1.value == step:
            pass
        if TimeFrame.M1.value == step:
            pass
        else:
            for base_end in range(len(base_bar_list) - 1, -1, -1):
                if 0 == (base_bar_list[base_end][0] + 60) % step_interval_sec:
                    break
    ret_bar_list = []
    bar = [0, 0, 0, 0, 0, 0]  # [ts, o, h, l, c, v]
    if TimeFrame.w1.value == step:
        interval_start = get_week_monday(base_bar_list[base_start][0])
        interval_end = interval_start + step_interval_sec
    if TimeFrame.M1.value == step:
        interval_start, interval_end = get_month_first_next_first(base_bar_list[base_start][0])
    else:
        interval_start = base_bar_list[base_start][0]
        interval_end = interval_start + step_interval_sec
    for base_index in range(base_start, base_end + 1):
        base_bar = base_bar_list[base_index]
        ts = base_bar[0]
        if interval_start <= ts < interval_end:  # 制作 bar
            if 0 == bar[0]:  # 初始值
                if TimeFrame.w1.value == step:
                    bar[0] = get_week_monday(base_bar[0])
                elif TimeFrame.M1.value == step:
                    bar[0], first_day_of_next_month = get_month_first_next_first(base_bar[0])
                else:
                    bar[0] = int(base_bar[0] / step_interval_sec) * step_interval_sec
                bar[1] = base_bar[1]
                bar[2] = base_bar[2]
                bar[3] = base_bar[3]
                bar[4] = base_bar[4]
                bar[5] = float(base_bar[5])
                # print("base_bar[0]: %s, bar[0]: %s., %s ~ %s." % (base_bar[0], bar[0], interval_start, interval_end))
            else:  # 更新值
                if 0 == bar[5] and float(base_bar[5]) != 0:
                    bar[1] = base_bar[1]  # 当前面的成交量一直为0，直到有成交时，修正当前时间范围的开盘价
                bar[2] = max(bar[2], base_bar[2])
                bar[3] = min(bar[3], base_bar[3])
                bar[4] = base_bar[4]
                bar[5] += float(base_bar[5])
        else:
            if bar[0] != 0:
                ret_bar_list.append(copy.copy(bar))  # 将制作好的 bar 放入 ret_bar_list
            if TimeFrame.w1.value == step:
                interval_start = get_week_monday(base_bar[0])
                interval_end = interval_start + step_interval_sec
            elif TimeFrame.M1.value == step:
                interval_start, interval_end = get_month_first_next_first(base_bar[0])
            else:
                interval_start = int(base_bar[0] / step_interval_sec) * step_interval_sec
                interval_end = interval_start + step_interval_sec
            bar[0] = interval_start
            bar[1] = base_bar[1]
            bar[2] = base_bar[2]
            bar[3] = base_bar[3]
            bar[4] = base_bar[4]
            bar[5] = float(base_bar[5])
            # print("base_bar[0]: %s, bar[0]: %s., %s ~ %s." % (base_bar[0], bar[0], interval_start, interval_end))
    ret_bar_list.append(copy.copy(bar))
    return ret_bar_list
