"""
Utility functions for the l2data_reader package.
"""

import os
import logging
import struct
import platform
import ctypes
import mmap
from typing import Optional

def configure_logging(
    level: int = logging.INFO,
    format_str: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    logger_name: str = "l2data_reader"
) -> logging.Logger:
    """
    配置并返回一个日志记录器。
    
    Args:
        level: 日志级别，默认为 INFO
        format_str: 日志格式字符串
        logger_name: 日志记录器名称
        
    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(logger_name)
    
    # 避免重复配置
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(format_str)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(level)
    return logger

def time_to_milliseconds(time_int: int) -> int:
    """
    将HHMMSSfff格式的时间转换为当日零点以来的毫秒数
    
    Args:
        time_int: HHMMSSfff格式的时间整数
        
    Returns:
        当日零点以来的毫秒数
    """
    hour = time_int // 10000000
    minute = (time_int // 100000) % 100
    second = (time_int // 1000) % 100
    millisecond = time_int % 1000
    return ((hour * 3600 + minute * 60 + second) * 1000 + millisecond)

def get_allocation_granularity() -> int:
    """
    获取系统的分配粒度（Allocation Granularity）。
    在 Windows 系统中，通常为 64KB。
    
    Returns:
        系统的分配粒度
    """
    if platform.system() != 'Windows':
        return mmap.PAGESIZE  # 对于非 Windows 系统，使用页面大小

    class SYSTEM_INFO(ctypes.Structure):
        _fields_ = [
            ("wProcessorArchitecture", ctypes.c_uint16),
            ("wReserved", ctypes.c_uint16),
            ("dwPageSize", ctypes.c_uint32),
            ("lpMinimumApplicationAddress", ctypes.c_void_p),
            ("lpMaximumApplicationAddress", ctypes.c_void_p),
            ("dwActiveProcessorMask", ctypes.POINTER(ctypes.c_uint)),
            ("dwNumberOfProcessors", ctypes.c_uint32),
            ("dwProcessorType", ctypes.c_uint32),
            ("dwAllocationGranularity", ctypes.c_uint32),
            ("wProcessorLevel", ctypes.c_uint16),
            ("wProcessorRevision", ctypes.c_uint16),
        ]

    sys_info = SYSTEM_INFO()
    ctypes.windll.kernel32.GetSystemInfo(ctypes.byref(sys_info))
    return sys_info.dwAllocationGranularity

def get_total_count(header_filepath: str) -> int:
    """
    从头文件（.hdr）中读取总记录数
    
    Args:
        header_filepath: 头文件路径
        
    Returns:
        市场数据记录的总数
        
    Raises:
        FileNotFoundError: 如果头文件不存在
        ValueError: 如果头文件无效
    """
    try:
        with open(header_filepath, 'rb') as f:
            # 使用内存映射读取文件
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # FileHeader 结构包含一个 int64_t 字段
                # 使用 'q' 格式表示 int64_t（8字节）
                total_count = struct.unpack('=q', mm.read(8))[0]
                return total_count
                
    except FileNotFoundError:
        raise FileNotFoundError(f"Header file not found: {header_filepath}")
    except Exception as e:
        raise ValueError(f"Failed to read header file: {str(e)}")