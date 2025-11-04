#!python3
# -*- coding:utf-8 -*-
import argparse
import json
import time
import threading
import requests
from datetime import datetime
import os
from jvUtil import HanqQing
import signal


class HqCenter:
    def __init__(self, args):
        self.token = args.token
        self.listen = args.listen
        self.init_codes_file = args.initCodesFile
        self.save_hq_file = args.saveHqFile

        # 数据初始化
        self.date = datetime.now().strftime('%Y-%m-%d')
        self.save_file = ""
        if self.save_hq_file:
            self.save_file = f"{self.save_hq_file}.{self.date}.txt"

        self.hq_sse_clients = []  # 行情订阅客户端
        self.log_sse_clients = []  # 日志订阅客户端
        self.op_log_arr = []  # 操作日志数组

        self.stop_event = threading.Event()

    def start(self):
        # 启动行情接收服务
        threading.Thread(target=self.hq_receive_service).start()

        print(f"行情中心已启动，监听地址: {self.listen}")

        # 等待停止信号
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        print("行情中心已退出")

    def signal_handler(self, signum, frame):
        print(f"收到退出信号: {signum}")
        self.stop_event.set()

    def hq_receive_service(self):
        # 行情接收服务
        def on_lv1(code, hq_map):
            self.on_hq_rev('lv1', code, hq_map)

        def on_lv2(code, hq_map):
            self.on_hq_rev('lv2', code, hq_map)

        def log_handle(*args):
            msg = f"{time.strftime('%H:%M:%S', time.localtime(time.time()))} {' '.join(map(str, args))}"
            self.op_log_arr.append(msg)
            print(msg)

        # 读取初始订阅代码
        init_codes = {'lv1': [], 'lv2': []}
        if self.init_codes_file and os.path.exists(self.init_codes_file):
            with open(self.init_codes_file, 'r', encoding='utf-8') as f:
                init_codes = json.load(f)

        # 连接行情服务器
        hq = HanqQing.Construct(log_handle, self.token, on_lv1, on_lv2)

        # 订阅初始代码
        if init_codes.get('lv1'):
            hq.addLv1(init_codes['lv1'])
        if init_codes.get('lv2'):
            hq.addLv2(init_codes['lv2'])

        while not self.stop_event.is_set():
            time.sleep(1)

        hq.close()

    def on_hq_rev(self, level, code, hq_map):
        # 行情接收回调
        try:
            hq_data = f"{code}={','.join(hq_map.values())}"

            # 保存行情数据
            if self.save_file:
                with open(self.save_file, 'a', encoding='utf-8') as f:
                    f.write(f"{hq_data}\t")

            # 广播给所有订阅客户端
            # 这里简化处理，实际应该通过HTTP SSE等方式推送
            print(f"收到{level}行情: {code}")

        except Exception as e:
            print(f"行情接收处理异常: {e}")


def main():
    parser = argparse.ArgumentParser(description='行情中心')
    parser.add_argument('--token', type=str, default='', help='jvQuant平台的访问token')
    parser.add_argument('--listen', type=str, default=':31800', help='http监听地址')
    parser.add_argument('--initCodesFile', type=str, default='./data/initCodes.json', help='启动即订阅的code')
    parser.add_argument('--saveHqFile', type=str, default='./data/hq',
                        help='行情写入文件,自动加日期后缀。为空则不写入文件。')

    args = parser.parse_args()

    hq_center = HqCenter(args)
    hq_center.start()


if __name__ == '__main__':
    main()
