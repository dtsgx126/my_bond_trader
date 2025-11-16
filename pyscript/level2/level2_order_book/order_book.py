#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
订单簿模块：实现上交所 Level 2 行情订单簿更新机制

支持如下功能：
1. 委托订单更新：
   - 当订单的 order_type 为 OrdActionType.ADD 时，根据订单的 order_side（Direction.BUY 或 Direction.SELL）
     将订单挂单数量累加到相应价格档位上，并保存订单映射。
   - 当订单的 order_type 为 OrdActionType.DELETE 时，根据订单的 seq_no 与挂单数量，
     从对应的价格档位上扣减挂单数量，并从订单映射中移除或更新该订单。

2. 成交更新：
   - 当成交 trade 的 trade_type 为 TrdType.UNKNOWN 时（委托撤单），报文仅包含买方或卖方订单号，
     则根据订单号从相应侧的档位扣减挂单数量。
   - 当成交 trade 的 trade_type 为 TrdType.UNKNOWN_N、TrdType.BUY 或 TrdType.SELL 时，
     报文同时包含买方与卖方订单号，则对两侧挂单数量进行扣减更新。
   
3. 快照功能：提供 snapshot 接口，返回订单簿买盘与卖盘各档的价格和挂单数量信息。

本模块仅依赖基础 Python 库以及从 enums.py 获取的相关枚举类型，不依赖其他模块。
"""

import logging

# 从 enums.py 中导入枚举类型
from l2data_reader.enums import (
    OrdActionType, Direction, TrdType
)

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


class Order:
    def __init__(self, seq_no, order_id, symbol, price, volume, side):
        """
        订单对象
        
        参数:
          seq_no: 订单序列号
          order_id: 订单ID
          symbol: 标的名称，如 "BTCUSDT"
          price: 委托价格
          volume: 委托数量
          side: 订单方向，取值为枚举 Direction.BUY 或 Direction.SELL
        """
        self.seq_no = seq_no
        self.order_id = order_id
        self.symbol = symbol
        self.price = price
        self.volume = volume
        self.side = side

    def __repr__(self):
        return (f"Order(seq={self.seq_no}, order_id={self.order_id}, symbol={self.symbol}, "
                f"side={self.side}, price={self.price}, vol={self.volume})")


class OrderBook:
    def __init__(self, symbol, snapshot_depth=-1):
        """
        初始化订单簿
        
        参数:
          symbol: 标的名称
          snapshot_depth: 快照返回的档位数量，默认为 -1
        """
        self.symbol = symbol
        self.snapshot_depth = snapshot_depth
        self.buy_levels = {}   # 买盘：键为价格，值为累计挂单量（降序显示）
        self.sell_levels = {}  # 卖盘：键为价格，值为累计挂单量（升序显示）
        self.order_map = {}    # 订单映射：order_id -> Order
        self.trade_date = 0
        self.transact_time = 0
        self.last_price = 0.0
        self.upper_limit_price = 9999999999.0
        self.lower_limit_price = 0.0
        
        # 初始化时保存一个空的快照，表示最后一次更新前的状态
        self.last_snapshot = {
            "symbol": str(self.symbol),
            "bid_price": [],
            "bid_volume": [],
            "ask_price": [],
            "ask_volume": [],
            "trade_date": self.trade_date,
            "transact_time": self.transact_time,
            "last_price": self.last_price,
            "upper_limit_price": self.upper_limit_price,
            "lower_limit_price": self.lower_limit_price
        }

    def _get_snapshot(self, depth=None):
        """
        内部方法：获取当前订单簿快照（不更新 last_snapshot）
        """
        if depth is None:
            depth = self.snapshot_depth
        # 买盘快照：按价格降序排序
        bids = sorted(self.buy_levels.items(), key=lambda x: x[0], reverse=True)
        bids = bids[:depth] if depth > 0 else bids        
        bid_prices = [price for price, volume in bids]
        bid_volumes = [volume for price, volume in bids]

        # 卖盘快照：按价格升序排序
        asks = sorted(self.sell_levels.items(), key=lambda x: x[0])
        asks = asks[:depth] if depth > 0 else asks        
        ask_prices = [price for price, volume in asks]
        ask_volumes = [volume for price, volume in asks]

        snapshot_data = {
            "symbol": str(self.symbol),
            "bid_price": bid_prices,
            "bid_volume": bid_volumes,
            "ask_price": ask_prices,
            "ask_volume": ask_volumes,
            "trade_date": self.trade_date,
            "transact_time": self.transact_time,
            "last_price": self.last_price,
            "upper_limit_price": self.upper_limit_price,
            "lower_limit_price": self.lower_limit_price,
        }
        return snapshot_data

    def snapshot(self, depth=None):
        """
        获取订单簿快照，返回格式与 Tick 数据类似，包含：
          - bid_price: 买盘价格列表（降序排列）
          - bid_volume: 买盘挂单数量列表
          - ask_price: 卖盘价格列表（升序排列）
          - ask_volume: 卖盘挂单数量列表
        
        同时返回更新前和更新后的快照，格式为 (previous_snapshot, current_snapshot)
        """
        current_snapshot = self._get_snapshot(depth)
        previous_snapshot = self.last_snapshot
        return previous_snapshot, current_snapshot
    
    def update_by_tick(self, ticks):
        """
        更新涨跌停价格
        """
        for tick in ticks:
            try:
                self.upper_limit_price = tick.get("upper_limit_price")
                self.lower_limit_price = tick.get("lower_limit_price")
            except Exception as e:
                logger.error(f"更新涨跌停价格时出错: {e}")

    def add_order(self, order):
        """
        添加订单，并将订单挂单数量聚合到订单簿中，同时记录在订单映射中
        """
        if order.side == Direction.BUY:
            self.buy_levels[order.price] = self.buy_levels.get(order.price, 0) + order.volume
            logger.debug(f"添加买单: {order}, 当前该价位聚合量: {self.buy_levels[order.price]}")
        elif order.side == Direction.SELL:
            self.sell_levels[order.price] = self.sell_levels.get(order.price, 0) + order.volume
            logger.debug(f"添加卖单: {order}, 当前该价位聚合量: {self.sell_levels[order.price]}")
        else:
            logger.warning(f"未知订单方向: {order.side}")
        self.order_map[order.order_id] = order

    def cancel_order(self, order):
        """
        根据撤单订单对象，从订单簿中扣减相应挂单数量，并在订单映射中删除该订单
        """
        if order.side == Direction.BUY:
            if order.price in self.buy_levels:
                self.buy_levels[order.price] -= order.volume
                logger.debug(f"撤销买单: {order}, 撤单后该价位剩余量: {self.buy_levels.get(order.price, 0)}")
                if self.buy_levels[order.price] <= 0:
                    del self.buy_levels[order.price]
            else:
                logger.warning(f"撤单失败，买盘无此价格: {order.price}")
        elif order.side == Direction.SELL:
            if order.price in self.sell_levels:
                self.sell_levels[order.price] -= order.volume
                logger.debug(f"撤销卖单: {order}, 撤单后该价位剩余量: {self.sell_levels.get(order.price, 0)}")
                if self.sell_levels[order.price] <= 0:
                    del self.sell_levels[order.price]
            else:
                logger.warning(f"撤单失败，卖盘无此价格: {order.price}")
        if order.order_id in self.order_map:
            del self.order_map[order.order_id]
        else:
            logger.warning(f"订单映射中未找到订单: {order.order_id}")

    def reduce_order(self, order_id, volume):
        """
        根据订单号从订单簿中扣减挂单数量（部分或全部）
        
        参数:
          order_id: 订单号
          volume: 扣减数量
          
        返回实际扣减的数量
        """
        order = self.order_map.get(order_id)
        if order is None:
            logger.warning(f"订单 {order_id} 在订单映射中未找到")
            return 0
        reduce_qty = min(order.volume, volume)
        if order.side == Direction.BUY:
            if order.price in self.buy_levels:
                self.buy_levels[order.price] -= reduce_qty
                if self.buy_levels[order.price] <= 0:
                    del self.buy_levels[order.price]
            else:
                logger.warning(f"买盘价位 {order.price} 不存在于订单簿中")
        elif order.side == Direction.SELL:
            if order.price in self.sell_levels:
                self.sell_levels[order.price] -= reduce_qty
                if self.sell_levels[order.price] <= 0:
                    del self.sell_levels[order.price]
            else:
                logger.warning(f"卖盘价位 {order.price} 不存在于订单簿中")
        order.volume -= reduce_qty
        if order.volume <= 0:
            del self.order_map[order_id]
        logger.debug(f"订单 {order_id} 扣减数量 {reduce_qty}")
        return reduce_qty

    def update_by_entrusts(self, entrusts):
        """
        根据逐笔委托列表更新订单簿。
        每个委托字典必须包含以下字段：
          - order_type: 枚举值，取值为 OrdActionType.ADD 或 OrdActionType.DELETE
          - order_side: 枚举值，取值为 Direction.BUY 或 Direction.SELL（或采用 "side" 字符串表示 Buy/Sell）
          - seq_no, symbol, price, volume
          
        当 order_type 为 OrdActionType.ADD 时，新增订单，
        当 order_type 为 OrdActionType.DELETE 时，根据 seq_no 扣减挂单数量（支持部分撤单）。
        更新时保存当前快照至 last_snapshot 以供比对。
        """
        self.last_snapshot = self._get_snapshot()
        for entrust in entrusts:
            order_type = entrust.get("order_type")
            seq_no = entrust.get("seq_no")
            order_id = entrust.get("order_id")
            symbol = entrust.get("symbol")
            price = entrust.get("price")
            volume = entrust.get("volume")
            order_side = entrust.get("order_side")
            if order_type == OrdActionType.ADD:
                order = Order(seq_no, order_id, symbol, price, volume, order_side)
                self.add_order(order)
            elif order_type == OrdActionType.DELETE:
                # 使用订单映射进行部分或全部撤单操作
                if order_id in self.order_map:
                    self.reduce_order(order_id, volume)
                else:
                    logger.warning(f"委托撤单: 序号 {order_id} 的订单不存在")
            elif order_type == OrdActionType.STATUS:
                logger.debug(f"产品状态订单: {entrust}")
            else:
                logger.warning(f"未知的 order_type: {order_type} 在委托 {entrust}")
                continue
            self.trade_date = entrust.get('trade_date', 0)
            self.transact_time = entrust.get('transact_time', 0)

    def process_trade(self, trade):
        """
        根据逐笔成交报文更新订单簿，报文要求包含以下字段：
          - trade_type: 枚举值，可能取值为 TrdType.UNKNOWN, TrdType.UNKNOWN_N, TrdType.BUY, TrdType.SELL
          - volume: 成交数量
          - buy_order_no 和/或 sell_order_no: 对应订单号
          
        处理规则：
          - 若 trade_type 为 TrdType.UNKNOWN（委托撤单），报文仅包含买方或卖方订单号，
            则仅对该侧进行扣减处理。
          - 若 trade_type 为 TrdType.UNKNOWN_N、TrdType.BUY 或 TrdType.SELL，
            则报文同时包含 buy_order_no 与 sell_order_no，对两侧订单分别进行扣减处理。
        
        返回字典，记录原始 trade 及各订单扣减情况。
        """
        deductions = []
        ttype = trade.get("trade_type")
        trade_volume = trade.get("volume", 0)
        if ttype == TrdType.UNKNOWN:
            if trade.get("buy_order_no") is not None:
                order_id = trade.get("buy_order_no")
                deducted = self.reduce_order(order_id, trade_volume)
                deductions.append({"order_id": order_id, "side": "Buy", "deducted": deducted})
            if trade.get("sell_order_no") is not None:
                order_id = trade.get("sell_order_no")
                deducted = self.reduce_order(order_id, trade_volume)
                deductions.append({"order_id": order_id, "side": "Sell", "deducted": deducted})
        elif ttype in [TrdType.UNKNOWN_N, TrdType.BUY, TrdType.SELL]:
            buy_order_no = trade.get("buy_order_no")
            sell_order_no = trade.get("sell_order_no")
            if buy_order_no is not None and sell_order_no is not None:
                deducted_buy = self.reduce_order(buy_order_no, trade_volume)
                deductions.append({"order_id": buy_order_no, "side": "Buy", "deducted": deducted_buy})
                deducted_sell = self.reduce_order(sell_order_no, trade_volume)
                deductions.append({"order_id": sell_order_no, "side": "Sell", "deducted": deducted_sell})
                self.last_price = trade.get("trade_price", 0)
            else:
                logger.warning(f"成交报文要求同时包含买方与卖方订单号: {trade}")
        else:
            logger.warning(f"未知的成交类型: {ttype}")
        return {"trade": trade, "deductions": deductions}

    def update_by_trades(self, trades):
        """
        根据逐笔成交列表更新订单簿，报文格式要求每个成交包含：
          - trade_type, volume, buy_order_no, sell_order_no
        更新前保存当前订单簿快照，返回每笔成交的扣减记录列表。
        """
        self.last_snapshot = self._get_snapshot()
        results = []
        for trade in trades:
            result = self.process_trade(trade)
            self.trade_date = trade.get('trade_date', 0)
            self.transact_time = trade.get('transact_time', 0)
            results.append(result)
        return results

    def __repr__(self):
        return (f"OrderBook(symbol={self.symbol}, "
                f"buy_levels={self.buy_levels}, sell_levels={self.sell_levels})")
