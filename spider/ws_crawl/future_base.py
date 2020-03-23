#! /usr/bin/python
# -*- coding:utf-8 -*-
# @zhuchen    : 2019-07-09 11:14

"""
期货基类
"""
import ujson

from django.conf import settings

from spider.ws_crawl import HoldBase, WS_TYPE_KLINE, WS_TYPE_TRADE
from exspider.utils.enum_con import RoutingKeyIncr, TimeFrame


class WSFutureBase(HoldBase):

    async def get_restful_bursts(self, symbol, start_id=None, end_id=None):
        """
        功能:
            获取 RESTFUL klines
        参数:
            start_id: 此 id 之后的数据
            end_id: 此 id 之前的数据
            start_id 与 end_id 同时传 以end_id 为准
        """
        url = await self.get_restful_burst_url(symbol, start_id, end_id)
        data = await self.get_http_data(url)
        ret = await self.parse_restful_burst(data)
        return ret

    async def get_restful_burst_url(self, symbol, start_id=None, end_id=None):
        """
        功能:
            获取 爆仓 url
        注意:
            需要 复写
        """
        api = self.http_data['api']
        path = self.http_data['urls']['bursts'].format(symbol)
        url = f'{api}{path}'
        return url

    async def parse_restful_burst(self, data):
        """
        功能:
            处理 爆仓订单
        """
        ...

    async def send_new_symbol_sub(self, pending_symbols, ws, ws_type=None):
        """
        功能:
            重连 以后 检测是否有新的订阅
            binance 这种的通过url订阅的, 只能先关闭, 再重新启动
        重要:
            因为合约 为每周交割, 所以 发送新订阅时, 重新初始化symbol
        """
        self.symbols = self.get_symbols()
        new_crawl_symbol = {x: self.crawl_symbol_map[x] for x in self.crawl_symbol_map if x in self.symbols}
        self.crawl_symbol_map = new_crawl_symbol
        pending_symbols = [x for x in pending_symbols if x in new_crawl_symbol]
        new_send_sub_datas = []
        if ws_type == WS_TYPE_TRADE:
            new_send_sub_datas = [
                await self.get_trade_sub_data(pending_symbols)
            ] if pending_symbols else []
        elif ws_type == WS_TYPE_KLINE:
            new_send_sub_datas = [
                await self.get_kline_sub_data(pending_symbols)
            ] if pending_symbols else []
        for sub_data in new_send_sub_datas:
            await ws.send_str(sub_data)

    async def save_trades_to_redis(self, symbol: str, trade_list: list, ws=None):
        """
        功能:
            统一处理入库
        注意:
            首次入库之前请求一次 restful 补数据
        """
        try:
            now = await self.now_timestamp
            if not trade_list:
                return
            if self.crawl_symbol_map[symbol].is_first_save:
                self.crawl_symbol_map[symbol].is_first_save = False
                await self.set_check_tms(symbol)
                await self.get_restful_trades(symbol, is_first_request=True)
            exchange_id = self.exchange_id
            await self.logger.debug(f'[{exchange_id}] {symbol} >> {trade_list[:2]}')
            # redis push and publish
            routing_key = await self.get_publish_routing_key(WS_TYPE_TRADE, exchange_id, symbol)
            msg = await self.get_publish_message(WS_TYPE_TRADE, exchange_id, symbol, trade_list)
            await self.publisher.publish(routing_key, msg)
            await self.redis_con.add_exchange_symbol_trades(exchange_id, self.symbols[symbol], trade_list)
            await self.set_update_tms(symbol, now)
            if now - self.crawl_symbol_map[symbol].check_tms > settings.CHECK_TRADES_TMS:
                # set check tms
                await self.set_check_tms(symbol)
                # check data
                await self.get_restful_trades(symbol)
            if now - self.crawl_symbol_map[symbol].csv_tms > settings.WRITE_TRADE_CSV_TMS:
                await self.set_csv_tms(symbol, now)
                # 发送csv生成指令
                ex_symbol = f'{self.symbols[symbol]}:{self.exchange_id}'
                await self.redis_con.push_csv_direct(ex_symbol, ws_type=WS_TYPE_TRADE)
        except Exception as e:
            await self.logger.error(e)

    async def save_kline_to_redis(self, symbol: str, kline, ws=None):
        """
        功能:
            保存 满柱kline到 redis 中写csv 实时的打入MQ中(满柱频道, 实时频道)
        """
        try:
            if not kline:
                return
            now = await self.now_timestamp
            # 首次&断开重连 删除缓存
            await self.set_update_tms(symbol, now)
            now_minute_tms = now - (now % 60)
            if self.crawl_symbol_map[symbol].is_first_save:
                self.crawl_symbol_map[symbol].is_first_save = False
                await self.set_check_tms(symbol)
                kline_list = await self.get_restful_klines(symbol, TimeFrame.m1.value, is_first_request=True)
                if kline_list:
                    start_check_tms = self.crawl_symbol_map[symbol].restful_kline_start_tms
                    self.crawl_symbol_map[symbol].restful_kline_start_tms = now_minute_tms
                    kline_list = kline_list if not start_check_tms else [x for x in kline_list if x[0] >= start_check_tms]
                    if now_minute_tms <= kline_list[-1][0]:
                        kline_list = kline_list[:-1]
                    await self.publish_kline(symbol, kline_list, incr=RoutingKeyIncr.history.value)

            if not self.crawl_symbol_map[symbol].last_ohlcv:
                self.crawl_symbol_map[symbol].last_ohlcv = kline
                await self.publish_kline(symbol, [kline], incr=RoutingKeyIncr.not_full.value)
                return
            # 缓存的上一条ohlcv
            last_ohlcv = self.crawl_symbol_map[symbol].last_ohlcv
            # 历史的丢弃
            if kline[0] < last_ohlcv[0]:
                return
            # 刷新最后一条ohlcv
            self.crawl_symbol_map[symbol].last_ohlcv = kline
            await self.logger.debug(f'[{self.exchange_id}]{self.uuid} {symbol} >> {kline}')

            if kline[0] > last_ohlcv[0]:
                # 新的一根开始, 先发布上一条, 然后下一条
                await self.publish_kline(symbol, [last_ohlcv], incr=RoutingKeyIncr.full.value)
            await self.publish_kline(symbol, [kline], incr=RoutingKeyIncr.not_full.value)

            if now - self.crawl_symbol_map[symbol].check_tms > settings.CHECK_KLINES_TMS:
                # set check tms
                await self.set_check_tms(symbol)
                # check data
                kline_list = await self.get_restful_klines(symbol, TimeFrame.m1.value)
                if kline_list:
                    if now_minute_tms == kline_list[-1][0]:
                        kline_list = kline_list[:-1]
                    await self.publish_kline(symbol, kline_list, incr=RoutingKeyIncr.history.value)
            if now - self.crawl_symbol_map[symbol].csv_tms > settings.WRITE_CSV_TMS:
                await self.set_csv_tms(symbol, now)
                ex_symbol = f'{self.symbols[symbol]}:{self.exchange_id}'
                await self.redis_con.push_csv_direct(ex_symbol, ws_type=WS_TYPE_KLINE)
        except Exception as e:
            await self.logger.error(e)

    async def publish_kline(self, symbol, klines, incr: RoutingKeyIncr=RoutingKeyIncr.not_full.value):
        """
        功能:
            发布kline, 满柱的 csv + mq, 其他的 mq
        """
        if not klines:
            return
        if incr in [RoutingKeyIncr.history.value, RoutingKeyIncr.full.value]:
            await self.redis_con.add_exchange_symbol_klines(self.exchange_id, self.symbols[symbol], klines)
        routing_key = await self.get_publish_routing_key(WS_TYPE_KLINE, self.exchange_id, symbol, incr=incr)
        for kline in klines[-3:]:
            publish_data = await self.get_publish_message(WS_TYPE_KLINE, self.exchange_id, symbol, kline)
            await self.publisher.publish(routing_key, publish_data)

    async def get_publish_routing_key(self, type, exchange_id, symbol, incr: RoutingKeyIncr=RoutingKeyIncr.not_full.value):
        """
        功能:
            获取 mq routing_key
        实例:
            "market.ticker.okex.btc.usdt.future.0.0"
        """
        _symbol = self.symbols[symbol]
        base, quote, future_type = _symbol.split('_')
        routing_key = f"market.{type}.{exchange_id}.{base}.{quote}.future.{future_type}.{incr}"
        return routing_key

    async def get_publish_message(self, type, exchange_id, symbol, data):
        """
        功能:
            封装 publish_data
            'c': 'kline',                                                                   # channel
            'e': 'huobipro',                                                                # 交易所id
            't': 'timestamp',                                                               # 时间戳
            's': 'btcusdt_weekly',                                                                 # 交易对
            'd': [1555289520, 5131.96, 5134.23, 5131.96, 5133.76, 10.509788169770346]       # 数据
        """
        info = {
            'c': type,
            'e': exchange_id,
            't': await self.now_timestamp,
            's': self.symbols[symbol],
            'd': data,
        }
        return ujson.dumps(info)