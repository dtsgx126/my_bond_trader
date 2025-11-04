import subprocess
import time
import signal
import sys
import os

processes = []


def signal_handler(signum, frame):
    """处理退出信号"""
    print(f"\n收到退出信号: {signum}")
    for p in processes:
        try:
            p.terminate()
        except:
            pass
    sys.exit(0)


def start_process(script_name, args=[]):
    """启动单个脚本进程"""
    cmd = [sys.executable, script_name] + args
    print(f"启动: {' '.join(cmd)}")
    process = subprocess.Popen(cmd)
    processes.append(process)
    return process


if __name__ == "__main__":
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # 启动各个服务
        start_process("hqCenter.py", [
            "--token", "your_token_here",
            "--listen", ":31800"
        ])

        time.sleep(2)  # 等待hqCenter启动

        start_process("orderHolder.py", [
            "--listen", ":31888"
        ])

        time.sleep(2)  # 等待orderHolder启动

        start_process("bondTrigger.py", [
            "--listen", ":31866",
            "--hqCenterAddr", "http://127.0.0.1:31800",
            "--tdCenterAddr", "http://127.0.0.1:31888"
        ])

        # 等待任意进程结束
        while True:
            time.sleep(1)
            if any(p.poll() is not None for p in processes):
                print("检测到进程退出，终止所有服务")
                break

    except KeyboardInterrupt:
        pass
    finally:
        signal_handler(signal.SIGTERM, None)
