#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-05-14 11:06

"""
文档: http://wiki.oa.ihold.com/pages/viewpage.action?pageId=11634122

功能:
    监控交易对价格变动

routing_key:
    "market.kline.okex.btc.usdt.spot.0.0"

data:
    'c': 'kline',                                                                   # channel
    'e': 'huobipro',                                                                # 交易所id
    't': 'timestamp',                                                               # 时间戳
    's': 'btcusdt',                                                                 # 交易对
    'd': [1555289520, 5131.96, 5134.23, 5131.96, 5133.76, 10.509788169770346]

"""
import asyncio
import ujson
import time
from collections import deque

from django.conf import settings

from exspider.utils.redis_con import AsyncRedisBase
from exspider.utils.logger_con import get_logger
from exspider.utils.http_base import HttpBase
from exspider.utils.email_con import Email
from exspider.common import funcs
from storekeeper.push_center import push_main
from exspider.utils.enum_con import PushType

# TODO 以下两个变量 在每个子类中完成
# # 交易对当前5分钟价格队列
# PAIR_PRICE_CACHE = {
#     # 'btcusdt:binance': PairCache
# }
#
# # 价格 突破整数
# PAIR_PRICE_PASS_CACHE = {
#     # 'btcusdt:binance': PricePass
# }

# 交易对价格精度
PAIR_PRICE_DIGITS_MAP = {
    'btcusdt:binance': 2
}

ENCODING = 'utf-8'
MESSAGE_PUSH_TYPE = 'remind'    # push 类型为价格异动


logger = get_logger(__name__, is_debug=settings.IS_DEBUG)
push_logger = get_logger(settings.APP_NAME)
email = Email()

# 交易所基础数据缓存
S_EXCHANGE_NAME_MAP = settings.EXCHANGE_NAME_MAP

S_IS_PUSH_OR_CHECK_PCT = settings.IS_PUSH_OR_CHECK_PCT

S_COIN_PUSH_FROM_PAIR_KEY = settings.COIN_PUSH_FROM_PAIR_KEY

S_PRICE_ALARM_COIN_LIST = funcs.get_alarm_price_coin()

S_PLUNGE_ALARM_PERCENT_MAP = funcs.get_alarm_price_push_percent()

S_PASS_PRICE_COIN_LIST = funcs.get_pass_price_coin()

S_PASS_PRICE_COIN_LIST.append('ETH')

S_SMALL_COIN_BOOM_ALARM_PCT = settings.SMALL_COIN_BOOM_ALARM_PCT

prod_redis_conn = None

# 利用trade 计算 kline 最后一条kline 的缓存
PAIR_LAST_TRADE_OHLCV = {

}

class PairCache:
    """
    说明:
        每个交易对的存储对象
    """
    pair_data = {
        'pair_id': 3890051,
        'coin_id': 301,
        'has_ohlcv': 1,
    }
    base_queue = None  # -> BaseQueue
    merge_queue = None  # -> MergeQueue
    merge_running = False   # 是否进入合并计算期


class BaseQueue:
    """
    说明:
        基础窗口
    """
    def __init__(self, exchange_id, base_name, quote_name, max_len=settings.PERIOD_PRICE_CHECK):
        self.max_len = max_len
        self.queue = deque(maxlen=self.max_len)                 # 基础只算5分钟
        self.is_push = False                                    # 是否触发push
        self.is_rise = False                                    # 是增长还是下跌
        self.exchange_id = exchange_id                          # binance
        self.base_name = base_name                              # btc
        self.quote_name = quote_name                            # usdt
        self.pair_name = f'{base_name}/{quote_name}'.upper()
        self.symbol_exchange = f'{base_name}{quote_name}:{exchange_id}'
        self.amount = 0                                         # 区间涨跌额度
        self.pct = 0                                            # 区间涨跌幅
        self.minutes_ago = 1                                    # 区间分钟
        self.now_price = 0                                      # 当前价格
        self.hit_data = None                                    # 触发push 的数据 [t, o, h, l, c, v] 涨: 取 l | 跌: 取 h
        self.digits = PAIR_PRICE_DIGITS_MAP.get(self.symbol_exchange, None)                                      # 小数点精度
        self.pair_data = None

    async def append(self, kline):
        """
        功能:
            1. 先添加数据到queue
            2. 然后计算
            3. 触发push 则push
        """
        if not self.digits:
            self.now_price = kline[4]
            await self.set_digits(kline)
            PAIR_PRICE_DIGITS_MAP[self.symbol_exchange] = self.digits
            await logger.info(f'{PAIR_PRICE_DIGITS_MAP}')
        else:
            self.now_price = format(kline[4], f'.{self.digits}f')
        await self.add_data_2_queue(kline)
        await self.calculation_price_update(kline)

    async def reset(self):
        self.queue = deque(maxlen=self.max_len)
        self.is_push = False  # 是否触发push
        self.is_rise = False
        self.amount = 0
        self.pct = 0  # 区间涨跌幅
        self.minutes_ago = 1  # 区间分钟
        self.now_price = 0  # 当前价格
        self.hit_data = None

    async def set_digits(self, kline):
        """
        功能:
            设置 精度
            判断交易所返回数据，如果小数位中间连续4位及以上出现0，则从出现0那一位截取
            上涨、下跌USDT精度与返回价格精度保持一致
        """
        global prod_redis_conn
        if not prod_redis_conn or prod_redis_conn.closed:
            _redis = AsyncRedisBase(config=settings.PROD_REDIS_CONF)
            prod_redis_conn = await _redis.redis
        redis_data = await prod_redis_conn.hget(settings.PAIR_PRECISION_KEY, self.symbol_exchange)
        if redis_data:
            digits = int(redis_data)
        else:
            digits = len(str(kline[1]).split('.')[1])
            if digits > 4:
                digits = 4
        self.digits = digits

    async def calculation_price_update(self, kline):
        """
        功能:
            计算函数
        """
        now_kline_tms = kline[0]
        last_low_price = kline[3]
        last_high_price = kline[2]
        min_price_kline = sorted((x for x in self.queue), key=lambda x: x[3])[0]
        max_price_kline = sorted((x for x in self.queue), key=lambda x: x[2], reverse=True)[0]
        min_price = min_price_kline[3]
        max_price = max_price_kline[2]
        rise_pct = round((last_high_price - min_price) / min_price * 100, 2)
        fall_pct = round((max_price - last_low_price) / last_low_price * 100, 2)
        await logger.debug(f'''{self.base_name}基础计算, 当前queue: {[x for x in self.queue]},涨幅: {rise_pct}, data: {
            min_price_kline}跌幅: {fall_pct}, data: {max_price_kline}''')
        base_pct = S_PLUNGE_ALARM_PERCENT_MAP.get(self.base_name.upper())
        if not base_pct:
            base_pct = settings.SMALL_COIN_BOOM_ALARM_PCT
        if rise_pct > base_pct:
            minutes_ago = (now_kline_tms - min_price_kline[0]) // 60
            if minutes_ago == 0:    # 如果 最新一条kline 最高最低触发, 那就按照开收判断 涨跌
                self.is_rise = True if kline[1] <= kline[4] else False
            else:
                self.is_rise = True
            if minutes_ago > self.max_len:
                return
            self.pct = abs(rise_pct)
            self.amount = format(last_high_price - min_price, f'.{self.digits}f')
            self.minutes_ago = minutes_ago if minutes_ago else 1
            self.hit_data = min_price_kline
            self.is_push = True
        elif fall_pct > base_pct:
            minutes_ago = (now_kline_tms - max_price_kline[0]) // 60
            if minutes_ago > self.max_len:
                return
            self.is_rise = False
            self.pct = abs(fall_pct)
            self.amount = format(max_price - last_low_price, f'.{self.digits}f')
            self.minutes_ago = minutes_ago if minutes_ago else 1
            self.hit_data = max_price_kline
            self.is_push = True
        if self.is_push:
            await self.push_message()

    async def add_data_2_queue(self, data):
        """
        功能:
            添加数据到queue
        data:
            [t, h, l]
        """
        if not self.queue:
            self.queue.append(data)
        elif data[0] == self.queue[-1][0]:
            self.queue[-1] = data
        elif data[0] > self.queue[-1][0]:
            self.queue.append(data)

    async def push_message(self):
        """
        功能:
            发送push 消息到PHP
        """
        await logger.info(f'{[x for x in self.queue]}')
        tms_str = await timestamp_2_str(self.queue[-1][0], is_day_str=True)
        title = f'{self.base_name.upper()} 行情异动'
        rise_str = '上涨' if self.is_rise else '下跌'
        exchange_name = S_EXCHANGE_NAME_MAP[self.exchange_id]['name']
        message = f"""【短时{rise_str}】据{exchange_name}行情，{self.base_name.upper()}过去{self.minutes_ago}分钟{
        rise_str}{self.pct}%，{rise_str}金额为{self.amount}{self.quote_name.upper()}，现报{self.now_price}。"""
        summary = f"""【短时{rise_str}】{self.base_name.upper()}过去{self.minutes_ago}分钟{
        rise_str}{self.pct}%，现报{self.now_price}。（数据来源：{exchange_name}）"""
        is_push = 1
        if self.base_name.upper() in S_PRICE_ALARM_COIN_LIST and self.pct >= S_IS_PUSH_OR_CHECK_PCT:
            is_push = 0
        push_data = {
            'uids': [],
            'title': title,
            'coin_id': self.pair_data['coin_id'],
            'body': message,
            'summary': summary,
            'jump': settings.PAIR_JUMP_URL.format(
                symbol=self.pair_name.replace('/', '_').lower(),
                exchange_id=self.exchange_id,
                pair_id=self.pair_data['pair_id'],
                hasOhlcv=self.pair_data['has_ohlcv'],
                coin_id=self.pair_data['coin_id'],
            ),
            'timestamp': int(time.time()),
            'type': 'PairAlarm',
            'is_push': is_push,
            'message_push_type': MESSAGE_PUSH_TYPE,
        }
        if not is_push:
            await email.async_send_email(settings.EDITOR_EMAIL_LIST, '待审核Push', message)
        call_php = {
            'call_func': settings.PHP_NOTICE_METHOD['pair_alarm'],
            'data': push_data
        }
        call_platform = {
            'call_func': '',
            'data': {
                'now_price': self.now_price,
                'minutes_ago': self.minutes_ago,
                'is_rise': self.is_rise,
                'pct': self.pct,
                'coin_id': self.pair_data['coin_id'],
            }
        }
        ret = await push_main(
                base_name=self.base_name, quote_name=self.quote_name, exchange_id=self.exchange_id,
                call_php=call_php, call_platform=call_platform, push_type=PushType.rise_or_fall.value)
        return ret


class MergeQueue(BaseQueue):
    """
    说明:
        合并计算窗口, 进去之后 每五分钟计算一次
    """
    def __init__(self, exchange_id, base_name, quote_name, max_len=settings.MERGE_PERIOD_PRICE_CHECK):
        super().__init__(exchange_id, base_name, quote_name, max_len)
        self.is_expired = False                                     # 是否过期(超过最大合并窗口)
        self.base_is_rise = False                                   # 基础窗口触发时的涨跌幅状态
        self.merge_is_rise = False                                  # 累积区间涨跌状态
        self.base_pct = 0                                           # 基础窗口触发时的涨跌幅
        self.window_start_tms = None                                # 本次开始计算的时间戳
        self.msg_data = None                                        # 用来缓存push消息
        self.other_pct = 0                                          # 冗余涨跌幅字段
        self.rise_and_fall_data = {                                 # 用来放急涨急跌的文案
            'date_time_str': '',
            'desc_str_1': '',
            'desc_str_2': '',
        }

    async def set_queue_len(self):
        """
        功能:
            改变queue 的长度
        """
        if not self.queue:
            self.queue = deque(maxlen=self.max_len)
        else:
            queue = deque(self.queue, maxlen=self.max_len)
            self.queue = queue
        start = await timestamp_2_str(self.queue[0][0], is_day_str=True)
        end = await timestamp_2_str(self.queue[-1][0], is_day_str=True)
        await logger.debug(f'''{self.base_name}当前queue长度: {len(self.queue), self.max_len}, {start}-{end}''')

    async def calculation_price_update(self, kline):
        """
        功能:
            计算函数
            1. 先判断合并是否结束, 中途数据是否断开
            2. 判断 是否计算 上五分钟 : 距离上一窗口大于5分钟
            3. 计算结果是否触发push: 窗口涨跌幅
            4. 合并计算 从开始合并到现在
            5. push
        """
        now_kline_tms = kline[0]
        if not self.queue:
            await logger.error(self.queue, 'Merge Queue is empty!!!')
            return
        # 如果合并期, 数据断开, 则合并结束
        minutes_ago = (now_kline_tms - self.hit_data[0]) // 60
        if minutes_ago > self.max_len:
            self.is_expired = True
            return
        self.minutes_ago = minutes_ago if minutes_ago else 1

        # 更新窗口开始时间
        window_len = settings.WINDOW_DURATION
        if self.base_name.upper() not in S_PRICE_ALARM_COIN_LIST:  # TODO 小币种窗口期为1小时
            window_len = settings.SMALL_WINDOW_DURATION
        math_queue = []
        if now_kline_tms > self.window_start_tms + window_len:
            math_queue = [x for x in self.queue if now_kline_tms > x[0] >= self.window_start_tms]  # 五分钟计算

        # 1. 每5分钟计算的区间 小币种 每1小时:
        if not math_queue:
            return
        # 缓存上次触发数据  注意: 首次合并使用基础数据
        last_pct = self.pct if self.pct else self.base_pct
        math_queue = sorted(math_queue, key=lambda x: x[0])
        math_open_price = math_queue[0][1]
        math_close_price = math_queue[-1][4]
        math_start_tms = math_queue[0][0]
        math_end_tms = math_queue[-1][0]
        math_pct = round((math_close_price - math_open_price) / math_open_price * 100, 2)
        await logger.debug(f'''{self.base_name}合并计算,当前queue: {math_queue},区间幅度: {math_pct}''')
        _base_pct = S_PLUNGE_ALARM_PERCENT_MAP.get(self.base_name.upper())
        if not _base_pct:
            _base_pct = S_SMALL_COIN_BOOM_ALARM_PCT
        if abs(math_pct) > _base_pct:
            self.is_rise = True if math_pct >= 0 else False
            self.is_push = True

        if not self.is_push:
            return

        # 2. 合并区间
        # 合并区间 最低价
        _queue_low_price = min([x[3] for x in self.queue][:-1])
        # 合并区间 最高价
        _queue_high_price = max([x[2] for x in self.queue][:-1])
        # 合并振幅
        self.other_pct = round((_queue_high_price - _queue_low_price) / _queue_low_price * 100, 2)
        # 合并累积涨跌幅 金额
        amount = math_close_price - self.hit_data[1]
        pct = round(amount / self.hit_data[1] * 100, 2)
        self.amount = format(abs(amount), f'.{self.digits}f')
        self.pct = abs(pct)
        self.merge_is_rise = True if pct >= 0 else False
        now_time_str = await timestamp_2_str(now_kline_tms, is_day_str=True)
        if self.is_rise == self.base_is_rise:
            await logger.debug(f'''{self.base_name}[持续涨跌]当前价: {self.now_price}, 区间: {self.minutes_ago}时间: {now_time_str},合并区间:开: {
                self.hit_data[1]},收: {math_queue[-1][4]},幅度: {self.pct},额度: {self.amount};振幅计算:最高价: {
                _queue_high_price}, 最低价: {_queue_low_price}, 振幅: {self.other_pct},''')

        else:
            # 跌 >> 涨 初始位置到上次合并
            datetime_start_str_1 = await timestamp_2_str(self.hit_data[0])
            datetime_end_str_1 = await timestamp_2_str(self.window_start_tms)

            datetime_start_str_2 = await timestamp_2_str(math_start_tms)
            datetime_end_str_2 = await timestamp_2_str(math_end_tms)

            if self.is_rise:
                desc_str_1 = f'{datetime_start_str_1}-{datetime_end_str_1}下跌{last_pct}%'
                desc_str_2 = f'{datetime_start_str_2}-{datetime_end_str_2}上涨{abs(math_pct)}%'
            else:
                desc_str_1 = f'{datetime_start_str_1}-{datetime_end_str_1}上涨{last_pct}%'
                desc_str_2 = f'{datetime_start_str_2}-{datetime_end_str_2}下跌{abs(math_pct)}%'
            self.rise_and_fall_data = {
                'date_time_str': now_time_str,
                'desc_str_1': desc_str_1,
                'desc_str_2': desc_str_2,
            }
            await logger.debug(f'''{self.base_name}[剧烈波动]当前价: {self.now_price}, 区间: {self.minutes_ago}时间: {now_time_str},追加区间:开: {
                self.hit_data[1]},收: {math_queue[-1][4]},幅度: {self.pct},额度: {self.amount};振幅计算:最高价: {
                _queue_high_price}, 最低价: {_queue_low_price},振幅: {self.other_pct}, 补充1: {
                desc_str_1},补充2: {desc_str_2},''')

        await self.push_message()
        self.base_is_rise = True if pct >= 0 else False
        self.window_start_tms = now_kline_tms
        # 每次push 后 计算周期都往后延30分钟
        self.max_len = len(self.queue) + settings.MERGE_PERIOD_PRICE_CHECK
        await self.set_queue_len()

    async def add_data_2_queue(self, data):
        """
        功能:
            判断合并计算是否过期
        """
        if not self.queue:
            self.queue.append(data)
        elif data[0] == self.queue[-1][0]:
            self.queue[-1] = data
        elif data[0] > self.queue[-1][0]:
            self.queue.append(data)
        if self.queue[-1][0] - self.hit_data[0] > self.max_len * 60:
            self.is_expired = True
        else:
            self.is_expired = False

    async def get_message_data(self):
        """
        功能:
            拼接 push msg
        """
        tms_str = await timestamp_2_str(self.queue[-1][0], is_day_str=True)
        title = f'{self.base_name.upper()} 行情异动'
        rise_str = '上涨' if self.is_rise else '下跌'
        exchange_name = S_EXCHANGE_NAME_MAP[self.exchange_id]['name']
        merge_rise_str = '上涨' if self.merge_is_rise else '下跌'
        if self.is_rise == self.base_is_rise:
            message = f'''【持续{rise_str}】据{exchange_name}行情，{self.base_name.upper()}过去{self.minutes_ago}分钟累计{
            merge_rise_str}{self.pct}%，{merge_rise_str}金额为{self.amount}{self.quote_name.upper()}，现报{self.now_price
            }，期间最高{"涨" if self.merge_is_rise else '跌'}幅为{self.other_pct}%。'''
            summary = f'''【持续{rise_str}】{self.base_name.upper()}过去{self.minutes_ago}分钟累计{
            merge_rise_str}{self.pct}%，现报{self.now_price
            }，期间最高{"涨" if self.merge_is_rise else '跌'}幅为{self.other_pct}%。（数据来源：{exchange_name}）'''
        else:
            if self.base_is_rise:
                first_str = '急涨急跌'
            else:
                first_str = '急跌急涨'
            message = f'''【{first_str}】据{exchange_name}行情，{self.base_name.upper()}短时剧烈波动，现报{self.now_price}，过去{
            self.minutes_ago}分钟累计{merge_rise_str}{self.pct}%，{merge_rise_str}金额为{self.amount}{self.quote_name.upper()
            }，期间最大波动幅度为{self.other_pct}%。<br>据统计，{
            self.rise_and_fall_data["desc_str_1"]}；{self.rise_and_fall_data["desc_str_2"]}。'''
            summary = f'''【{first_str}】{self.base_name.upper()}短时剧烈波动，现报{self.now_price}，过去{
            self.minutes_ago}分钟累计{merge_rise_str}{self.pct}%，期间最大波动幅度为{self.other_pct}%。据统计，{
            self.rise_and_fall_data["desc_str_1"]}；{self.rise_and_fall_data["desc_str_2"]}。（数据来源：{exchange_name}）'''
        self.msg_data = {
            'title': title,
            'message': message,
            'summary': summary
        }
        return True

    async def push_message(self):
        await self.get_message_data()
        title = self.msg_data['title']
        message = self.msg_data['message']
        summary = self.msg_data['summary']
        is_push = 1 if self.pct < S_IS_PUSH_OR_CHECK_PCT and self.other_pct < S_IS_PUSH_OR_CHECK_PCT else 0
        push_data = {
            'uids': [],
            'title': title,
            'coin_id': self.pair_data['coin_id'],
            'body': message,
            'summary': summary,
            'jump': settings.PAIR_JUMP_URL.format(
                symbol=self.pair_name.replace('/', '_').lower(),
                exchange_id=self.exchange_id,
                pair_id=self.pair_data['pair_id'],
                hasOhlcv=self.pair_data['has_ohlcv'],
                coin_id=self.pair_data['coin_id'],
            ),
            'timestamp': int(time.time()),
            'type': 'PairAlarm',
            'is_push': is_push,
            'message_push_type': MESSAGE_PUSH_TYPE,
        }
        if not is_push:
            await email.async_send_email(settings.EDITOR_EMAIL_LIST, '待审核Push', message)
        call_php = {
            'call_func': settings.PHP_NOTICE_METHOD['pair_alarm'],
            'data': push_data
        }
        call_platform = {
            'call_func': '',
            'data': {
                'now_price': self.now_price,
                'minutes_ago': self.minutes_ago,
                'is_rise': self.is_rise,
                'pct': self.pct,
                'coin_id': self.pair_data['coin_id'],
            }
        }
        # push 完成后重置push 状态
        self.is_push = False
        ret = await push_main(
            base_name=self.base_name, quote_name=self.quote_name, exchange_id=self.exchange_id,
            call_php=call_php, call_platform=call_platform, push_type=PushType.rise_or_fall.value)
        return ret


class PricePass:
    """
    说明:
        价格 突破整数关口
    """

    def __init__(self, exchange_id, base_name, quote_name):
        self.exchange_id = exchange_id                              # binance
        self.base_name = base_name                                  # btc
        self.quote_name = quote_name                                # usdt
        self.symbol_exchange = f'{base_name}{quote_name}:{exchange_id}'
        self.pair_name = f'{base_name}/{quote_name}'.upper()
        self.pair_data = {}
        self.pass_cache = []                                        # 突破 缓存
        self._rise_pass_price = None                                      # 待 突破 整数关口
        self._fall_pass_price = None                                      # 待 跌破 整数关口
        self.now_price = None
        self.end_merge_tms = None
        self.is_rise = False
        self.pass_price = None
        self._pass_base_integer = settings.PAIR_PASS_PRICE_BASE_MAP.get(base_name.upper(), 100)
        self._pass_window = settings.PRICE_PASS_WINDOW * 60

    async def check_price(self, price, tms):
        """
        功能:
            检测是否突破
            1. 如果超过 最后统计时间, 则重新计算
            2. 首次按照当前价生成的突破口
            3. 首次突破关口直接push, 并重新计算
            4. 突破的关口重复, 则需要判断是不是已经下一个关口, 更新突破口
            5. push 后 恢复突破口到默认
        """
        if self.end_merge_tms and tms > self.end_merge_tms:
            self.end_merge_tms = None
            self._fall_pass_price = None
            self.pass_cache = []
        self.now_price = price
        if not self._fall_pass_price:
            self._fall_pass_price = price // self._pass_base_integer * self._pass_base_integer
            self._rise_pass_price = self._fall_pass_price + self._pass_base_integer
        is_push = False
        if self.now_price >= self._rise_pass_price:
            # 突破
            self.is_rise = True
            self.pass_price = self._rise_pass_price
            if self.pass_price not in self.pass_cache:
                is_push = True
            else:
                # 如果突破口已经存在 判断突破口是否改变
                now_rise_pass_price = self._rise_pass_price + self._pass_base_integer
                if self.now_price > now_rise_pass_price:
                    self.pass_price = now_rise_pass_price
                    if self.pass_price not in self.pass_cache:
                        is_push = True
            self._rise_pass_price = self.pass_price + self._pass_base_integer
            self._fall_pass_price = self.pass_price

        elif self.now_price <= self._fall_pass_price:
            # 跌破
            self.is_rise = False
            self.pass_price = self._fall_pass_price
            if self.pass_price not in self.pass_cache:
                is_push = True
            else:
                # 如果突破口已经存在 判断突破口是否改变
                now_fall_pass_price = self._fall_pass_price - self._pass_base_integer
                if self.now_price < now_fall_pass_price:
                    self.pass_price = now_fall_pass_price
                    if self.pass_price not in self.pass_cache:
                        is_push = True
            self._fall_pass_price = self.pass_price - self._pass_base_integer
            self._rise_pass_price = self.pass_price

        if is_push:
            # self.pass_cache.append(self.pass_price)   # TODO 2019年05月28日14:21:25 杨辰, 每次push 后 刷新缓存
            self.pass_cache = [self.pass_price]
            self.end_merge_tms = tms + self._pass_window
            await self.push_message(tms)
            tms_str = await timestamp_2_str(tms, is_day_str=True)
            await logger.debug(f'{tms_str} 关口记录: 当前已触发队列: {self.pass_cache} | 下次关口: {self._fall_pass_price, self._rise_pass_price}')

    async def push_message(self, tms):
        tms_str = await timestamp_2_str(tms, is_day_str=True)
        title = f'{self.base_name.upper()} 行情异动'
        exchange_name = S_EXCHANGE_NAME_MAP[self.exchange_id]['name']
        rise_str = '突破' if self.is_rise else '跌破'
        ticker = await get_pair_ticker(self.exchange_id, f'{self.base_name}{self.quote_name}')
        if ticker:
            change_24 = ticker['change_24h']
            change_pct_24h = ticker['change_pct_24h']
            rise_str_2 = '涨' if change_24 >= 0 else '跌'
            pct = change_pct_24h.replace("-", "").replace("%", "")
            sub_str = f'，过去24小时{rise_str_2}幅为{pct}%'
        else:
            sub_str = ''
            pct = 0
            rise_str_2 = '涨'
        message = f"""【{rise_str}关口】据{exchange_name}行情，{self.base_name.upper()}短时{rise_str}{int(self.pass_price)}{self.quote_name.upper()}，现报{
        self.now_price}{sub_str}。"""
        summary = f"""【{rise_str}关口】{self.base_name.upper()}短时{rise_str}{int(self.pass_price)}{self.quote_name.upper()}，现报{
        self.now_price}{sub_str}。（数据来源：{exchange_name}）"""
        push_data = {
            'uids': [],
            'title': title,
            'coin_id': self.pair_data['coin_id'],
            'body': message,
            'summary': summary,
            'jump': settings.PAIR_JUMP_URL.format(
                symbol=self.pair_name.replace('/', '_').lower(),
                exchange_id=self.exchange_id,
                pair_id=self.pair_data['pair_id'],
                hasOhlcv=self.pair_data['has_ohlcv'],
                coin_id=self.pair_data['coin_id'],
            ),
            'timestamp': int(time.time()),
            'type': 'PairAlarm',
            'is_push': 1 if float(pct) < 20 else 0,
            'message_push_type': MESSAGE_PUSH_TYPE,
        }
        call_php = {
            'call_func': settings.PHP_NOTICE_METHOD['pair_alarm'],
            'data': push_data
        }
        call_platform = {
            'call_func': '',
            'data': {
                'now_price': self.now_price,
                'is_rise': self.is_rise,
                'pass_price': self.pass_price,
                'coin_id': self.pair_data['coin_id'],
                'pct': pct,
                'rise_str_2': rise_str_2,
            }
        }
        ret = await push_main(
            base_name=self.base_name, quote_name=self.quote_name, exchange_id=self.exchange_id,
            call_php=call_php, call_platform=call_platform, push_type=PushType.pass_price.value)
        return ret


class SleepMergeQueue(BaseQueue):
    """
    说明：
        对于没有合并周期的币种， 采用次方法进行区间暂停
    """
    def __init__(self, exchange_id, base_name, quote_name, max_len=settings.MERGE_PERIOD_PRICE_CHECK):
        super().__init__(exchange_id, base_name, quote_name, max_len)
        self.is_expired = False                                     # 是否过期(超过最大合并窗口)
        self.base_is_rise = False                                   # 基础窗口触发时的涨跌幅状态
        self.merge_is_rise = False                                  # 累积区间涨跌状态
        self.base_pct = 0                                           # 基础窗口触发时的涨跌幅
        self.window_start_tms = None                                # 本次开始计算的时间戳
        self.msg_data = None                                        # 用来缓存push消息
        self.other_pct = 0                                          # 冗余涨跌幅字段
        self.rise_and_fall_data = {                                 # 用来放急涨急跌的文案
            'date_time_str': '',
            'desc_str_1': '',
            'desc_str_2': '',
        }

    async def append(self, kline):
        """
        功能:
            1. 先添加数据到queue
            2. 然后计算
            3. 触发push 则push
        """
        if not self.queue:
            await logger.error(self.queue, 'Merge Queue is empty!!!')
            return
        minutes_ago = (kline[0] - self.hit_data[0]) // 60
        if minutes_ago > self.max_len:
            self.is_expired = True
            return


class PairAlarmBase:

    """
    说明:
        价格 异动 类
    """

    def __init__(self, loop=None, is_test=False):
        self._loop = loop if loop else asyncio.get_event_loop()
        self._redis = AsyncRedisBase(loop=self._loop)
        self._cur = None
        self._is_test = is_test
        self.logger = logger

    async def pair_alarm_msg_process_func(self, routing_key, data):
        global prod_redis_conn
        if not self._cur or self._cur.closed:
            self._cur = await self._redis.redis
        if not prod_redis_conn or prod_redis_conn.closed:
            _redis = AsyncRedisBase(loop=self._loop, config=settings.PROD_REDIS_CONF)
            prod_redis_conn = await _redis.redis
        # "market.kline.okex.btc.usdt.spot.0.0"
        _split_routing_key = routing_key.split('.')
        msg_type = _split_routing_key[1]
        if 'kline' == msg_type:
            # base_name = _split_routing_key[3]
            # if base_name.upper() not in S_PRICE_ALARM_COIN_LIST:     # 只计算 配置了 价格预警的币种
            #     return
            data_json = ujson.loads(data)
            kline = data_json['d']
            data_json['d'] = []
            await self.alarm_main(routing_key, data_json, kline)
        elif 'trade' == msg_type:
            # base_name = _split_routing_key[3]
            # if base_name.upper() not in S_PRICE_ALARM_COIN_LIST:  # 只计算 配置了 价格预警的币种
            #     return
            data_json = ujson.loads(data)
            trades = data_json['d']
            symbol = data_json['s']
            exchange_id = data_json['e']
            data_json['d'] = []
            exchange_symbol = f'{symbol}:{exchange_id}'.lower()
            for kline in await self.parse_trade_2_kline(exchange_symbol, trades):
                await self.alarm_main(routing_key, data_json, kline)

    async def parse_trade_2_kline(self, exchange_symbol: str, trades: list) -> list:
        """
        功能:
            用返回的trade 生成kline
        :param trades: [t_list2, t_list2]
        :return: kline_list: [kline1, kline2]
        trade = [t, id, type, price, amount]
        """
        global PAIR_LAST_TRADE_OHLCV
        klines = {}
        for trade in trades:
            t = trade[0] // 60 * 60
            price = trade[3]
            amount = trade[4]
            kline = [t, price, price, price, price, amount]
            if exchange_symbol not in PAIR_LAST_TRADE_OHLCV:
                PAIR_LAST_TRADE_OHLCV[exchange_symbol] = kline
            else:
                last_kline = PAIR_LAST_TRADE_OHLCV[exchange_symbol]
                print(t, amount, last_kline)
                if t == last_kline[0]:
                    new_kline = [
                        t,
                        last_kline[1],
                        max(last_kline[2], price),
                        min(last_kline[3], price),
                        price,
                        amount + last_kline[5]
                    ]
                elif t > last_kline[0]:
                    new_kline = kline
                else:
                    continue
                klines[t] = new_kline
                print(new_kline)
                PAIR_LAST_TRADE_OHLCV[exchange_symbol] = new_kline
        return sorted(klines.values(), key=lambda x: x[0])

    async def alarm_main(self, routing_key, data, kline):
        """
        功能:
            复写 方法,  针对不同的监控模式
        :param routing_key:
        :param data:
        :return:
        """
        return
        if not self._is_test:
            if kline[0] < int(time.time()) // 60 * 60:
                return
        _split_routing_key = routing_key.split('.')
        base_name = _split_routing_key[3]
        quote_name = _split_routing_key[4]
        exchange_id = data['e']
        symbol = data['s']
        kline = data['d']
        exchange_symbol = f'{symbol}:{exchange_id}'
        if not await self.check_pair_is_active(base_name, exchange_symbol):   # 币种是否绑定了 当前交易对源
            if exchange_symbol in PAIR_PRICE_CACHE:
                PAIR_PRICE_CACHE.pop(exchange_symbol)
            if exchange_symbol in PAIR_PRICE_PASS_CACHE:
                PAIR_PRICE_PASS_CACHE.pop(exchange_symbol)
            return
        if base_name.upper() in S_PASS_PRICE_COIN_LIST: # 判断 币种 是否开启 整数关口预警
            if exchange_symbol not in PAIR_PRICE_PASS_CACHE:
                pair_pass = PricePass(exchange_id, base_name, quote_name)
                pair_pass.pair_data = await get_exchange_pair_data(exchange_id, base_name, quote_name, loop=self._loop)
                PAIR_PRICE_PASS_CACHE[exchange_symbol] = pair_pass
            try:
                await PAIR_PRICE_PASS_CACHE[exchange_symbol].check_price(price=kline[4], tms=kline[0])
            except Exception as e:
                await logger.error(e)

        if exchange_symbol not in PAIR_PRICE_CACHE:
            pair_cache = PairCache()
            pair_cache.pair_data = await get_exchange_pair_data(exchange_id, base_name, quote_name, loop=self._loop)
            pair_cache.base_queue = BaseQueue(exchange_id, base_name, quote_name)
            pair_cache.base_queue.pair_data = pair_cache.pair_data
            PAIR_PRICE_CACHE[exchange_symbol] = pair_cache
        else:
            pair_cache = PAIR_PRICE_CACHE[exchange_symbol]
        if pair_cache.merge_running:
            # 合并计算
            await pair_cache.merge_queue.append(kline)
            if pair_cache.merge_queue.is_expired or pair_cache.merge_queue.is_push:
                # 如果合并计算结束, 则重新进行普通计算
                PAIR_PRICE_CACHE.pop(exchange_symbol)
        else:
            # 普通窗口计算
            await pair_cache.base_queue.append(kline)
            if pair_cache.base_queue.is_push:
                pair_cache.merge_running = True
                insert_base_dates = [x for x in pair_cache.base_queue.queue if x[0] >= pair_cache.base_queue.hit_data[0]]
                merge_queue_len = len(insert_base_dates) + settings.MERGE_PERIOD_PRICE_CHECK # 合并计算的分钟数
                pair_cache.merge_queue = MergeQueue(exchange_id, base_name, quote_name, max_len=merge_queue_len)
                pair_cache.merge_queue.pair_data = pair_cache.pair_data
                pair_cache.merge_queue.queue.extend(insert_base_dates)
                pair_cache.merge_queue.hit_data = pair_cache.base_queue.hit_data
                pair_cache.merge_queue.base_is_rise = pair_cache.base_queue.is_rise
                pair_cache.merge_queue.window_start_tms = pair_cache.base_queue.queue[-1][0]
                pair_cache.merge_queue.base_pct = pair_cache.base_queue.pct

    async def check_pair_is_active(self, base_name, exchange_symbol):
        """
        功能:
            判断 是否 push 价格异动
        """
        r_data = await self._cur.hget(S_COIN_PUSH_FROM_PAIR_KEY, base_name, encoding=ENCODING)
        if r_data and r_data == exchange_symbol:
            return True
        return False


async def timestamp_2_str(timestamp, format='%H:%M', is_day_str=False):
    """
    功能:
        时间戳 转换成固定字符串
    实例:
        timestamp: 1557987600
        return: 5月16日14:20
    """
    struct_time = time.localtime(timestamp)
    if is_day_str:
        time_str = f"{struct_time.tm_mon}月{struct_time.tm_mday}日{time.strftime('%H:%M', struct_time)}"
    else:
        time_str = time.strftime(format, struct_time)
    return time_str


async def get_exchange_pair_data(exchange_id, base_name, quote_name, loop=None):
    """
    功能:
        调用内部api 获取交易对信息
    """
    symbol = f'{base_name}{quote_name}'.lower()
    if exchange_id in S_EXCHANGE_NAME_MAP and symbol in S_EXCHANGE_NAME_MAP[exchange_id]['pairs']:
        return S_EXCHANGE_NAME_MAP[exchange_id]['pairs'][symbol]
    else:
        exchange_data_api = f'{settings.C21_CANDLES_API}/pair/infos?exchange_id={exchange_id}&base_name={base_name}&quote_name={quote_name}'
        http = HttpBase(loop=loop)
        data = {}
        for i in range(3):
            response_data = await http.get_json_data(exchange_data_api)
            if response_data:
                data = response_data.get('data', {})
                break
        ret_data = {
            'pair_id': data.get('pair_id', ''),
            'coin_id': data.get('coin_id', ''),
            'has_ohlcv': data.get('has_ohlcv', '')
        }
        exchange_name = data.get('exchange_name', '')
        if exchange_id not in S_EXCHANGE_NAME_MAP:
            S_EXCHANGE_NAME_MAP[exchange_id] = {
                'name': exchange_name or str.capitalize(exchange_id),
                'pairs': {
                    symbol: ret_data
                }
            }

        S_EXCHANGE_NAME_MAP[exchange_id]['pairs'].update({symbol: ret_data})
        await logger.info(S_EXCHANGE_NAME_MAP[exchange_id])
        return ret_data


async def get_pair_ticker(exchange_id, symbol):
    key = f'{exchange_id}_handle_ticker'
    ticker_data = await prod_redis_conn.hget(key, symbol.upper())
    if not ticker_data:
        return None
    return ujson.loads(ticker_data)
