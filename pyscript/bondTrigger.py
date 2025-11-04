#!python3
# -*- coding:utf-8 -*-
import argparse
import json
import time
import threading
import requests
from datetime import datetime
import os
from jvUtil import HanqQing, Trade
import signal
import sys


class BondTrigger:
    def __init__(self, args):
        self.listen = args.listen
        self.hq_center_addr = args.hqCenterAddr
        self.td_center_addr = args.tdCenterAddr
        self.local_cb_addr = args.localCbAddr
        self.replay_file = args.rePlay
        self.bond_file = args.bondFile
        self.shares_file = args.sharesFile
        self.conf_file = args.confFile

        # 数据初始化
        self.stock_bond_map = {}
        self.bond_stock_map = {}
        self.stock_shares_map = {}
        self.code_latest_hq_map = {}
        self.bond_amount_per_sec_map = {}
        self.select_conf = []

        # 线程同步对象
        self.stop_event = threading.Event()

        # 加载配置数据
        self.data_init()

    def data_init(self):
        # 加载正股-转债映射
        with open(self.bond_file, 'r', encoding='utf-8') as f:
            self.bond_stock_map = json.load(f)
            # 构建反向映射
            for bond, stock in self.bond_stock_map.items():
                if stock not in self.stock_bond_map:
                    self.stock_bond_map[stock] = []
                self.stock_bond_map[stock].append(bond)

        # 加载正股-流通股映射
        with open(self.shares_file, 'r', encoding='utf-8') as f:
            self.stock_shares_map = json.load(f)

        # 读取触发条件配置
        with open(self.conf_file, 'r', encoding='utf-8') as f:
            self.select_conf = json.load(f)

    def start(self):
        # 启动行情服务
        threading.Thread(target=self.hq_service).start()

        # 启动订单监控服务
        threading.Thread(target=self.order_watch_service).start()

        # 启动持仓监控服务
        threading.Thread(target=self.hold_watch_service).start()

        print(f"债券触发器已启动，监听地址: {self.listen}")

        # 等待停止信号
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        print("债券触发器已退出")

    def signal_handler(self, signum, frame):
        print(f"收到退出信号: {signum}")
        self.stop_event.set()

    def hq_service(self):
        # 连接行情中心获取实时行情
        def on_lv1(code, hq_map):
            self.parse_lv1(code, hq_map)

        def on_lv2(code, hq_map):
            # LV2行情暂未接入
            pass

        def log_handle(*args):
            print(time.strftime('%H:%M:%S', time.localtime(time.time())), args)

        # 订阅所有正股和转债行情
        all_codes = list(self.stock_bond_map.keys()) + list(self.bond_stock_map.keys())

        hq = HanqQing.Construct(log_handle, "", on_lv1, on_lv2)
        hq.addLv1(all_codes)

        while not self.stop_event.is_set():
            time.sleep(1)

        hq.close()

    def parse_lv1(self, code, hq_map):
        # 解析LV1行情数据
        try:
            price = float(hq_map.get('price', 0))
            ratio = float(hq_map.get('ratio', 0))
            amount = float(hq_map.get('amount', 0))
            volume = float(hq_map.get('volume', 0))
            time_str = hq_map.get('time', '')

            # 保存最新行情
            self.code_latest_hq_map[code] = {
                'code': code,
                'time': time_str,
                'price': price,
                'ratio': ratio,
                'amount': amount,
                'volume': volume,
                'hq_map': hq_map
            }

            # 如果是正股行情，进行触发判断
            if code in self.stock_bond_map:
                self.select_stock(code, hq_map)

        except Exception as e:
            print(f"解析行情数据异常: {e}")

    def select_stock(self, code, hq_map):
        # 正股触发逻辑
        try:
            price = float(hq_map.get('price', 0))
            ratio = float(hq_map.get('ratio', 0))
            amount = float(hq_map.get('amount', 0))
            volume = float(hq_map.get('volume', 0))
            time_str = hq_map.get('time', '')

            # 简化的触发条件检查
            for conf_id, conf in enumerate(self.select_conf):
                # 检查涨幅条件
                if ratio >= conf.get('raRate', 0):
                    print(f"[{conf_id}]正股触发:{hq_map.get('name', '')},{code},时间:{time_str},涨幅:{ratio}")

                    # 选择对应的转债
                    bond = self.select_bond_from_stock(conf_id, code, conf)
                    if bond:
                        # 触发买入
                        self.buy_ctl(conf_id, bond, conf)

        except Exception as e:
            print(f"正股触发判断异常: {e}")

    def select_bond_from_stock(self, conf_id, code, conf):
        # 根据正股选择转债
        if code not in self.stock_bond_map:
            return ""

        bonds = self.stock_bond_map[code]
        selected_bond = ""
        max_amount = 0

        # 选择成交最活跃的转债
        for bond in bonds:
            if bond in self.code_latest_hq_map:
                hq_data = self.code_latest_hq_map[bond]
                amount = hq_data.get('amount', 0)
                if amount > max_amount and amount >= conf.get('bondAmt', 0):
                    max_amount = amount
                    selected_bond = bond

        return selected_bond

    def buy_ctl(self, conf_id, bond, conf):
        # 控制买入逻辑
        try:
            if bond not in self.code_latest_hq_map:
                return

            hq_data = self.code_latest_hq_map[bond]
            price = float(hq_data['hq_map'].get('s1p', 0)) * (100 + conf.get('bUpper', 0)) / 100

            if price == 0:
                return

            vol = conf.get('vol', 0)
            if vol == 0:
                vol = self.get_bond_proper_vol(conf.get('amt', 0), price)

            if vol == 0:
                return

            # 构造买入请求
            params = {
                'key': f'[{conf_id}]@{json.dumps(conf)}',
                'code': bond,
                'name': hq_data['hq_map'].get('name', ''),
                'price': f'{price:.3f}',
                'vol': str(int(vol)),
                'cb': self.local_cb_addr,
                'timeout': str(conf.get('bWait', 3))
            }

            print(
                f"[{conf_id}]买入请求: {hq_data['hq_map'].get('name', '')}, {bond}, 价格: {price:.3f}, 数量: {int(vol)}")

            # 回放模式不进行实际交易
            if self.replay_file:
                return

            # 发送买入请求到orderHolder
            url = f"{self.td_center_addr}/buy"
            response = requests.get(url, params=params, timeout=5)
            result = response.json()

            if result.get('code') == '0' and result.get('order_id'):
                print(
                    f"[{conf_id}]买单发出: {hq_data['hq_map'].get('name', '')}, {bond}, 价格: {price:.3f}, 数量: {int(vol)}, 单号: {result['order_id']}")
            else:
                print(f"[{conf_id}]买单异常: {result.get('message', '')}")

        except Exception as e:
            print(f"买入控制异常: {e}")

    def get_bond_proper_vol(self, amt, price):
        # 计算合适的买入数量
        if price == 0:
            return 0
        return int(amt / 10 / price) * 10

    def order_watch_service(self):
        # 订单监控服务
        while not self.stop_event.is_set():
            try:
                # 定期刷新订单状态
                time.sleep(1)
            except Exception as e:
                print(f"订单监控异常: {e}")
                time.sleep(1)

    def hold_watch_service(self):
        # 持仓监控服务
        while not self.stop_event.is_set():
            try:
                # 监控持仓止盈止损
                time.sleep(1)
            except Exception as e:
                print(f"持仓监控异常: {e}")
                time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description='债券交易触发器')
    parser.add_argument('--listen', type=str, default=':31866', help='http监听地址')
    parser.add_argument('--hqCenterAddr', type=str, default='http://127.0.0.1:31800', help='行情服务器地址')
    parser.add_argument('--tdCenterAddr', type=str, default='http://127.0.0.1:31888', help='委托服务器地址')
    parser.add_argument('--localCbAddr', type=str, default='http://127.0.0.1:31866/cb',
                        help='对orderHolder提供的回调地址')
    parser.add_argument('--rePlay', type=str, default='', help='向hqCenter指定回放行情文件')
    parser.add_argument('--bondFile', type=str, default='../data/select.json', help='正股-转债映射文件,json格式')
    parser.add_argument('--sharesFile', type=str, default='../data/shares.json',
                        help='正股-流通股映射文件,用来计算换手率,json格式')
    parser.add_argument('--confFile', type=str, default='../data/trigger.json', help='触发条件配置文件')

    args = parser.parse_args()

    trigger = BondTrigger(args)
    trigger.start()


if __name__ == '__main__':
    main()
