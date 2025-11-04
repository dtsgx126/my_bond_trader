#!python3
# -*- coding:utf-8 -*-
import argparse
import json
import time
import threading
import requests
from datetime import datetime
import os
from jvUtil import Trade
import signal


class OrderHolder:
    def __init__(self, args):
        self.listen = args.listen
        self.order_interval = args.orderInterval
        self.hold_interval = args.holdInterval
        self.token = args.token
        self.td_acc = args.acc
        self.td_pwd = args.pwd
        self.ticket_file = args.ticketFile
        self.order_log = args.orderLog

        # 订单管理数据
        self.to_check_oid_map = {}  # 待检查的订单ID映射
        self.key_item_map = {}  # 关键状态项映射

        self.stop_event = threading.Event()

        # 初始化交易接口
        self.trade = Trade.Construct(
            token=self.token,
            acc=self.td_acc,
            pwd=self.td_pwd,
            ticket="",
            server=""
        )

    def start(self):
        # 启动交易凭证服务
        threading.Thread(target=self.trade_ticket_service).start()

        # 启动订单监控服务
        threading.Thread(target=self.order_watch_service).start()

        # 启动持仓监控服务
        threading.Thread(target=self.hold_watch_service).start()

        print(f"订单管理器已启动，监听地址: {self.listen}")

        # 等待停止信号
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        print("订单管理器已退出")

    def signal_handler(self, signum, frame):
        print(f"收到退出信号: {signum}")
        self.stop_event.set()

    def trade_ticket_service(self):
        # 交易凭证服务
        while not self.stop_event.is_set():
            try:
                now_stamp = int(time.time())
                expire = 9000  # 2.5小时

                # 读取现有凭证
                ticket_info = []
                if os.path.exists(self.ticket_file):
                    with open(self.ticket_file, 'r', encoding='utf-8') as f:
                        ticket_info = json.load(f)

                ticket = ""
                last_ticket_stamp = 0
                server = ""

                if len(ticket_info) == 3:
                    ticket = ticket_info[0]
                    last_ticket_stamp = int(ticket_info[1])
                    server = ticket_info[2]

                # 检查是否需要更新凭证
                update = False
                if now_stamp - last_ticket_stamp >= expire - 60:  # 提前60秒刷新
                    ticket = ""
                    server = ""
                    update = True
                    print("即将动态刷新交易凭证...")

                # 重新初始化交易接口
                self.trade = Trade.Construct(
                    token=self.token,
                    acc=self.td_acc,
                    pwd=self.td_pwd,
                    ticket=ticket,
                    server=server
                )

                # 更新凭证文件
                if update:
                    new_ticket_info = [
                        self.trade.getTicket(),
                        str(now_stamp),
                        self.trade.server if hasattr(self.trade, 'server') else ''
                    ]
                    with open(self.ticket_file, 'w', encoding='utf-8') as f:
                        json.dump(new_ticket_info, f)

                time.sleep(30)

            except Exception as e:
                print(f"交易凭证服务异常: {e}")
                time.sleep(30)

    def buy(self, key, code, name, price, vol, cb, timeout):
        # 买入委托
        try:
            if key in self.key_item_map:
                return {"code": "-1", "message": f"该key已存在,请更换key后再试:{code} {name} {key}"}

            now_stamp = int(time.time())
            timeout_int = int(timeout) if timeout.isdigit() else 3

            # 发送买入请求
            rsp = self.trade.buy(code, name, price, vol)

            if rsp.get('code') == '0' and rsp.get('order_id'):
                # 记录订单信息
                key_item = {
                    'key': key,
                    'code': code,
                    'name': name,
                    'boid': rsp['order_id'],
                    'b_status': '未报',
                    'b_o_stamp': now_stamp,
                    'b_o_time': datetime.now().strftime('%H:%M:%S')
                }

                self.key_item_map[key] = key_item
                self.to_check_oid_map[rsp['order_id']] = {
                    'key': key,
                    'type': '证券买入',
                    'in_stamp': now_stamp,
                    'in_time': datetime.now().strftime('%H:%M:%S'),
                    'oid': rsp['order_id'],
                    'timeout': timeout_int,
                    'cb': cb
                }

                print(
                    f"买单请求 {name},code:{code},price:{price},vol:{vol},timeout:{timeout_int},单号:{rsp['order_id']}")
            else:
                print(
                    f"买单异常 {name},code:{code},price:{price},vol:{vol},timeout:{timeout_int},异常:{rsp.get('message', '')}")

            return rsp

        except Exception as e:
            print(f"买入委托异常: {e}")
            return {"code": "-1", "message": str(e)}

    def sale(self, key, code, name, price, vol, cb, timeout):
        # 卖出委托
        try:
            if key not in self.key_item_map:
                return {"code": "-1", "message": f"不存在该key,请检查后再试:{code} {name} {key}"}

            key_item = self.key_item_map[key]

            # 检查是否已有卖单在处理
            if key_item.get('s_status') in ['已报', '部成', '未报']:
                return {"code": "-1", "message": f"该key已有卖单在处理,请撤单完成后再试:{code} {name} {key}"}

            now_stamp = int(time.time())
            timeout_int = int(timeout) if timeout.isdigit() else 3

            # 发送卖出请求
            rsp = self.trade.sale(code, name, price, vol)

            if rsp.get('code') == '0' and rsp.get('order_id'):
                # 更新卖单信息
                if not key_item.get('fsoid'):
                    key_item['fsoid'] = rsp['order_id']

                key_item['s_o_stamp'] = now_stamp
                key_item['s_status'] = '未报'
                key_item['soid'] = rsp['order_id']

                self.key_item_map[key] = key_item
                self.to_check_oid_map[rsp['order_id']] = {
                    'key': key,
                    'type': '证券卖出',
                    'in_stamp': now_stamp,
                    'in_time': datetime.now().strftime('%H:%M:%S'),
                    'oid': rsp['order_id'],
                    'timeout': timeout_int,
                    'cb': cb
                }

                print(
                    f"卖单请求 {name},code:{code},price:{price},vol:{vol},timeout:{timeout_int},单号:{rsp['order_id']}")
            else:
                print(
                    f"卖单异常 {name},code:{code},price:{price},vol:{vol},timeout:{timeout_int},异常:{rsp.get('message', '')}")

            return rsp

        except Exception as e:
            print(f"卖出委托异常: {e}")
            return {"code": "-1", "message": str(e)}

    def order_watch_service(self):
        # 订单监控服务
        while not self.stop_event.is_set():
            try:
                if not self.to_check_oid_map:
                    time.sleep(1)
                    continue

                # 查询订单状态
                order_info = self.trade.check_order()
                if order_info.get('code') != '0':
                    print(f"查询交易异常: {order_info.get('message', '')}")
                    time.sleep(self.order_interval)
                    continue

                now_stamp = int(time.time())

                # 处理每个待检查的订单
                for oid, o_map in list(self.to_check_oid_map.items()):
                    # 查找订单信息
                    order_item = None
                    for item in order_info.get('list', []):
                        if item.get('order_id') == oid:
                            order_item = item
                            break

                    if not order_item:
                        print(f"未查询到order信息: {oid}")
                        continue

                    key = o_map['key']
                    if key not in self.key_item_map:
                        print(f"未查询到keyItem信息: {key}, {oid}")
                        continue

                    key_item = self.key_item_map[key]

                    # 更新订单状态
                    if order_item.get('type') == '证券买入':
                        key_item.update({
                            'name': order_item.get('name', ''),
                            'b_status': order_item.get('status', ''),
                            'b_o_price': float(order_item.get('order_price', 0)),
                            'b_o_volume': float(order_item.get('order_volume', 0)),
                            'b_d_price': float(order_item.get('deal_price', 0)),
                            'b_d_volume': float(order_item.get('deal_volume', 0))
                        })

                    elif order_item.get('type') == '证券卖出':
                        key_item.update({
                            's_status': order_item.get('status', ''),
                            's_o_price': float(order_item.get('order_price', 0)),
                            's_o_volume': float(order_item.get('order_volume', 0)),
                            's_d_price': float(order_item.get('deal_price', 0))
                        })

                    # 检查订单是否完成
                    status = order_item.get('status', '')
                    if status in ['已成', '已撤', '部撤']:
                        # 更新成交时间
                        if order_item.get('type') == '证券买入':
                            key_item['b_d_stamp'] = now_stamp
                        elif order_item.get('type') == '证券卖出':
                            key_item['s_d_stamp'] = now_stamp
                            key_item['s_d_time'] = datetime.now().strftime('%H:%M:%S')

                            # 累计卖出数量
                            if key_item.get('fsoid') != key_item.get('soid'):
                                key_item['s_d_volume'] = key_item.get('s_d_volume', 0) + float(
                                    order_item.get('deal_volume', 0))
                            else:
                                key_item['s_d_volume'] = float(order_item.get('deal_volume', 0))

                        # 回调通知
                        try:
                            callback_data = json.dumps(key_item)
                            callback_rsp = requests.post(
                                o_map['cb'],
                                data={'data': callback_data},
                                timeout=5
                            )
                            print(
                                f"{order_item.get('type')} {order_item.get('name')} {status},回调完成:{callback_rsp.text},回调地址:{o_map['cb']}")
                        except Exception as e:
                            print(
                                f"{order_item.get('type')} {order_item.get('name')} {status},回调失败:{e},回调地址:{o_map['cb']}")

                        # 从检查列表中移除
                        del self.to_check_oid_map[oid]

                    # 检查是否超时需要撤单
                    elif now_stamp - o_map['in_stamp'] >= o_map['timeout']:
                        status = order_item.get('status', '')
                        if status in ['未报', '已报', '部成']:
                            print(
                                f"{order_item.get('type')} {order_item.get('name')},单号:{oid},状态:{status},{o_map['timeout']}秒后未成自动撤单")
                            cancel_rsp = self.trade.cancel(oid)
                            if cancel_rsp.get('code') != '0':
                                print(
                                    f"{order_item.get('type')} {order_item.get('name')}撤单异常:{oid},{cancel_rsp.get('message', '')}")

                    # 更新key_item_map
                    self.key_item_map[key] = key_item

                time.sleep(self.order_interval)

            except Exception as e:
                print(f"订单监控异常: {e}")
                time.sleep(self.order_interval)

    def hold_watch_service(self):
        # 持仓监控服务
        while not self.stop_event.is_set():
            try:
                # 检查是否有需要监控的持仓
                need_check_keys = []
                now_stamp = int(time.time())

                for key, key_item in self.key_item_map.items():
                    # 买入已完成但未卖出，且距离买入5秒以上
                    if (key_item.get('b_status') in ['已成', '部撤'] and
                            not key_item.get('s_status') and
                            now_stamp - key_item.get('b_d_stamp', 0) > 5):
                        need_check_keys.append(key)

                if not need_check_keys:
                    time.sleep(1)
                    continue

                # 查询持仓情况
                hold_info = self.trade.check_hold()
                if hold_info.get('code') != '0':
                    print(f"查询持仓异常: {hold_info.get('message', '')}")
                    time.sleep(self.hold_interval)
                    continue

                # 构建持仓映射
                code_hold_map = {}
                for item in hold_info.get('hold_list', []):
                    code_hold_map[item.get('code', '')] = item

                # 检查是否手动清仓
                for key in need_check_keys:
                    if key not in self.key_item_map:
                        continue

                    key_item = self.key_item_map[key]
                    code = key_item.get('code', '')

                    if code in code_hold_map:
                        hold_item = code_hold_map[code]
                        if hold_item.get('hold_vol', 0) == 0:
                            # 已清仓，更新状态
                            key_item.update({
                                'fsoid': 'hand',
                                'soid': 'hand',
                                's_o_stamp': now_stamp,
                                's_d_stamp': now_stamp,
                                's_d_time': datetime.now().strftime('%H:%M:%S'),
                                's_status': '已成',
                                's_d_volume': key_item.get('b_d_volume', 0)
                            })

                            # 计算收益
                            buy_price = key_item.get('b_d_price', 0)
                            key_item['s_o_price'] = buy_price
                            key_item['s_d_price'] = buy_price
                            key_item['earn'] = round((buy_price - buy_price) * key_item.get('b_d_volume', 0), 2)

                            self.key_item_map[key] = key_item
                            print(f"检测到清仓:{key_item.get('name', '')},{code},{key}")

                time.sleep(self.hold_interval)

            except Exception as e:
                print(f"持仓监控异常: {e}")
                time.sleep(self.hold_interval)


def main():
    parser = argparse.ArgumentParser(description='订单管理器')
    parser.add_argument('--listen', type=str, default=':31888', help='http服务器监听地址')
    parser.add_argument('--orderInterval', type=int, default=2, help='委托单查询间隔')
    parser.add_argument('--holdInterval', type=int, default=5, help='手动清仓检测间隔')
    parser.add_argument('--token', type=str, default='', help='jvToken')
    parser.add_argument('--acc', type=str, default='', help='资金账户')
    parser.add_argument('--pwd', type=str, default='', help='资金密码')
    parser.add_argument('--ticketFile', type=str, default='./data/ticket.tmp', help='交易凭证临时存储文件')
    parser.add_argument('--orderLog', type=str, default='./data/orders.json', help='服务退出时保留的交易历史文件')

    args = parser.parse_args()

    order_holder = OrderHolder(args)
    order_holder.start()


if __name__ == '__main__':
    main()
