"""
Core functionality for reading market data.
"""

import os
import time
import struct
from dataclasses import dataclass
from typing import Optional, Tuple, Any, Dict, List, Union
import logging
from google.protobuf.message import Message

# 使用 utils.py 中的功能
from .utils import time_to_milliseconds, get_total_count, get_allocation_granularity
# 使用 exceptions.py 中的异常类
from .exceptions import NoDataException, DataFormatException, FileAccessException, ProtobufParseException
# 使用公共内存映射类
from .mmap_utils import MemoryMappedFile

from .proto.market_data_pb2 import (
    Envelope,
    SecuDepthMarketData,
    TransactionEntrustData,
    TransactionTradeData
)

@dataclass
class MarketDataHeader:
    sequence_no: int
    timestamp: int
    msg_type: int
    body_length: int

    @classmethod
    def from_bytes(cls, data: bytes) -> 'MarketDataHeader':
        if len(data) < cls.size():
            raise ValueError("Insufficient data for MarketDataHeader")
        # C++ 端的 MarketDataHeader 结构：int64_t, int64_t, uint32_t, uint32_t
        sequence_no, timestamp, msg_type, body_length = struct.unpack('=qqII', data)
        return cls(sequence_no, timestamp, msg_type, body_length)

    @staticmethod
    def size() -> int:
        return struct.calcsize('=qqII')  # 8 + 8 + 4 + 4 = 24 字节

@dataclass
class MarketDataResult:
    """市场数据读取结果，包含头部信息和数据内容"""
    sequence_no: int
    timestamp: int
    msg_type: int
    envelope: Envelope  # Protobuf Envelope 对象
    
    @property
    def header(self) -> MarketDataHeader:
        """获取头部信息"""
        return MarketDataHeader(
            sequence_no=self.sequence_no,
            timestamp=self.timestamp,
            msg_type=self.msg_type,
            body_length=self.envelope.ByteSize()
        )
    
    @property
    def data(self) -> Envelope:
        """获取数据内容，保持向后兼容"""
        return self.envelope
    
    @property
    def is_tick_data(self) -> bool:
        """是否为行情快照数据"""
        from .proto.market_data_pb2 import MessageTypeEnum
        return self.msg_type == MessageTypeEnum.MSG_SECU_DEPTH_MARKET_DATA
    
    @property
    def is_order_data(self) -> bool:
        """是否为委托数据"""
        from .proto.market_data_pb2 import MessageTypeEnum
        return self.msg_type == MessageTypeEnum.MSG_TRANSACTION_ENTRUST
    
    @property
    def is_trade_data(self) -> bool:
        """是否为成交数据"""
        from .proto.market_data_pb2 import MessageTypeEnum
        return self.msg_type == MessageTypeEnum.MSG_TRANSACTION_TRADE
    
    @property
    def tick_data(self) -> Optional[SecuDepthMarketData]:
        """获取行情快照数据"""
        if self.is_tick_data and hasattr(self.envelope, 'secu_depth_market_data'):
            return self.envelope.secu_depth_market_data
        return None
    
    @property
    def order_data(self) -> Optional[TransactionEntrustData]:
        """获取委托数据"""
        if self.is_order_data and hasattr(self.envelope, 'transaction_entrust_data'):
            return self.envelope.transaction_entrust_data
        return None
    
    @property
    def trade_data(self) -> Optional[TransactionTradeData]:
        """获取成交数据"""
        if self.is_trade_data and hasattr(self.envelope, 'transaction_trade_data'):
            return self.envelope.transaction_trade_data
        return None

@dataclass
class IndexEntry:
    sequence_no: int
    offset: int

    @classmethod
    def from_bytes(cls, data: bytes) -> 'IndexEntry':
        if len(data) < cls.size():
            raise ValueError("Insufficient data for IndexEntry")
        # C++ 端的 IndexEntry 结构：int64_t, uint64_t
        sequence_no, offset = struct.unpack('=qQ', data)
        return cls(sequence_no, offset)

    @staticmethod
    def size() -> int:
        return struct.calcsize('=qQ')  # 8 + 8 = 16 字节

def get_related_files(bin_file):
    """根据 .bin 文件路径自动推导 .idx 与 .hdr 文件路径"""
    base_path = os.path.splitext(bin_file)[0]
    idx_file = base_path + '.idx'
    hdr_file = base_path + '.hdr'
    return idx_file, hdr_file

class MarketDataReader:
    """行情数据读取器，使用内存映射方式读取二进制行情数据文件"""
    
    def __init__(self, bin_file: str, logger=None):
        """
        初始化行情数据读取器
        
        参数:
            bin_file: 数据文件路径（包含.bin扩展名）
            logger: 日志记录器，如果为None则创建默认记录器
        """
        self.logger = logger or logging.getLogger(__name__)
        self.bin_file = bin_file
        self.idx_file, self.hdr_file = get_related_files(bin_file)
        for f in [self.bin_file, self.idx_file, self.hdr_file]:
            if not os.path.exists(f):
                self.logger.error(f"文件不存在: {f}，自动构建")
        
        # 内存映射文件对象
        self.data_mmap = None
        self.index_mmap = None
        
        # 顺序读取相关状态
        self.current_sequence = -1  # 当前读取的序列号
        self.last_index_size = 0    # 上次读取时的索引文件大小
        self.read_count = 0         # 已读取的记录数
        self.first_read = True      # 是否是首次读取
        
        # 初始化
        self._init_reader()
    
    def _init_reader(self):
        """初始化读取器，打开文件并创建内存映射"""
        try:
            # 检查文件是否存在
            for file_path in [self.bin_file, self.idx_file, self.hdr_file]:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"文件不存在: {file_path}")
            
            # 获取记录总数
            total_count = get_total_count(self.hdr_file)
            
            # 创建数据文件内存映射
            self.data_mmap = MemoryMappedFile(
                logger=self.logger,
                filename=self.bin_file,
                window_size=max(1024*1024*10, get_allocation_granularity()),  # 10MB窗口大小
                write_mode=False
            )
            
            # 创建索引文件内存映射
            self.index_mmap = MemoryMappedFile(
                logger=self.logger,
                filename=self.idx_file,
                window_size=max(1024*1024*2, get_allocation_granularity()),  # 2MB窗口大小
                write_mode=False
            )
            
            self.logger.info(f"行情数据读取器初始化成功，总记录数: {total_count}")
            
        except Exception as e:
            self.logger.error(f"初始化行情数据读取器失败: {str(e)}")
            self.close()
            raise

    def get_count(self) -> int:
        """获取最新的文件大小"""
        try:
            return get_total_count(self.hdr_file)
        except Exception as e:
            self.logger.error(f"获取文件大小失败: {str(e)}")
            raise

    def get_size(self) -> int:
        """获取最新的文件大小"""
        return self.data_mmap.get_size()
    
    def get_index_entry(self, sequence_no: int) -> Optional[IndexEntry]:
        """获取指定序列号的索引条目"""
        total_count = self.get_count()
        if sequence_no < 0 or sequence_no >= total_count:
            self.logger.warning(f"序列号超出范围: {sequence_no}, 总记录数: {total_count}")
            return None
        
        try:
            # 计算索引条目在索引文件中的偏移量
            offset = sequence_no * IndexEntry.size()
            
            # 读取索引条目数据
            data = self.index_mmap.read_at(offset, IndexEntry.size())
            if not data:
                self.logger.error(f"读取索引条目失败: 序列号={sequence_no}, 偏移量={offset}")
                return None
            
            # 解析索引条目
            return IndexEntry.from_bytes(data)
            
        except Exception as e:
            self.logger.error(f"获取索引条目异常: 序列号={sequence_no}, 错误={str(e)}")
            return None
    
    def get_market_data(self, sequence_no: int) -> Optional[MarketDataResult]:
        """获取指定序列号的行情数据"""
        try:
            # 获取索引条目
            index_entry = self.get_index_entry(sequence_no)
            if not index_entry:
                return None
            
            # 读取数据头部
            header_data = self.data_mmap.read_at(index_entry.offset, MarketDataHeader.size())
            if not header_data:
                self.logger.error(f"读取数据头部失败: 序列号={sequence_no}, 偏移量={index_entry.offset}")
                return None
            
            # 解析数据头部
            header = MarketDataHeader.from_bytes(header_data)
            
            # 检查序列号是否匹配
            if header.sequence_no != sequence_no:
                self.logger.error(f"数据头部序列号不匹配: 预期={sequence_no}, 实际={header.sequence_no}")
                return None
            
            # 读取数据体
            body_offset = index_entry.offset + MarketDataHeader.size()
            body_data = self.data_mmap.read_at(body_offset, header.body_length)
            if not body_data or len(body_data) != header.body_length:
                self.logger.error(f"读取数据体失败: 序列号={sequence_no}, 偏移量={body_offset}, "
                              f"预期长度={header.body_length}, 实际长度={len(body_data) if body_data else 0}")
                return None
            
            # 解析 Protobuf 消息
            envelope = Envelope()
            try:
                envelope.ParseFromString(body_data)
            except Exception as e:
                self.logger.error(f"解析 Protobuf 消息失败: 序列号={sequence_no}, 错误={str(e)}")
                raise ProtobufParseException(f"解析 Protobuf 消息失败: {str(e)}")
            
            # 创建结果对象
            result = MarketDataResult(
                sequence_no=header.sequence_no,
                timestamp=header.timestamp,
                msg_type=header.msg_type,
                envelope=envelope
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"获取行情数据异常: 序列号={sequence_no}, 错误={str(e)}")
            return None
    
    def get_market_data_range(self, start_seq: int, end_seq: int = None, max_count: int = 1000) -> List[MarketDataResult]:
        """获取指定范围的行情数据"""
        results = []
        
        # 参数检查和调整
        if start_seq < 0:
            start_seq = 0
        
        total_count = self.get_count()
        if end_seq is None:
            end_seq = total_count - 1
        elif end_seq >= total_count:
            end_seq = total_count - 1
        
        # 限制最大返回数量
        count = min(end_seq - start_seq + 1, max_count)
        end_seq = start_seq + count - 1
        
        # 读取数据
        for seq in range(start_seq, end_seq + 1):
            result = self.get_market_data(seq)
            if result:
                results.append(result)
        
        return results
    
    def get_market_data_by_time(self, start_time: int, end_time: int = None, max_count: int = 1000) -> List[MarketDataResult]:
        """根据时间范围获取行情数据（二分查找）"""
        total_count = self.get_count()
        if total_count == 0:
            return []
        
        # 如果未指定结束时间，使用当前时间
        if end_time is None:
            end_time = int(time.time() * 1000)
        
        # 二分查找开始位置
        start_idx = self._binary_search_time(start_time, 0, total_count - 1, True)
        if start_idx == -1:
            return []
        
        # 二分查找结束位置
        end_idx = self._binary_search_time(end_time, start_idx, total_count - 1, False)
        if end_idx == -1:
            end_idx = total_count - 1
        
        # 限制返回数量
        count = min(end_idx - start_idx + 1, max_count)
        end_idx = start_idx + count - 1
        
        # 读取数据
        return self.get_market_data_range(start_idx, end_idx, count)
    
    def _binary_search_time(self, target_time: int, left: int, right: int, find_first: bool) -> int:
        """二分查找指定时间的记录索引"""
        result = -1
        
        while left <= right:
            mid = (left + right) // 2
            
            # 获取中间位置的记录时间
            index_entry = self.get_index_entry(mid)
            if not index_entry:
                return -1
            
            header_data = self.data_mmap.read_at(index_entry.offset, MarketDataHeader.size())
            if not header_data:
                return -1
            
            header = MarketDataHeader.from_bytes(header_data)
            mid_time = header.timestamp
            
            if find_first:
                # 查找第一个大于等于目标时间的记录
                if mid_time >= target_time:
                    result = mid
                    right = mid - 1
                else:
                    left = mid + 1
            else:
                # 查找最后一个小于等于目标时间的记录
                if mid_time <= target_time:
                    result = mid
                    left = mid + 1
                else:
                    right = mid - 1
        
        return result

    def read_next(self) -> tuple[MarketDataHeader, Envelope]:
        """
        顺序读取下一条行情数据，适用于连续读取场景
        
        返回:
            (header, envelope): 数据头部和消息体的元组，如果无新数据则返回None
        """
        try:
            # 获取最新的索引文件大小
            current_index_size = os.path.getsize(self.idx_file)
            if current_index_size == self.last_index_size:
                return None  # 无新数据
            
            # 如果是第一次读取，打印可用数据条数
            if self.first_read:
                total_entries = current_index_size // IndexEntry.size()
                self.logger.debug(f"首次读取，共有 {total_entries} 条市场数据可用")
                self.first_read = False
            
            # 确保索引文件有足够的数据可读
            if self.last_index_size + IndexEntry.size() > current_index_size:
                return None  # 索引数据不完整
            
            # 计算当前索引条目的偏移量
            index_offset = self.last_index_size
            
            # 读取索引项数据
            index_data = self.index_mmap.read_at(index_offset, IndexEntry.size())
            if not index_data or len(index_data) < IndexEntry.size():
                self.logger.warning("读取到不完整的索引数据，等待更多数据...")
                return None  # 数据不完整，等待新数据
            
            # 解析索引项
            index_entry = IndexEntry.from_bytes(index_data)
            
            # 检查序列号，避免重复读取
            if index_entry.sequence_no <= self.current_sequence:
                if index_entry.sequence_no != 0:
                    self.logger.debug(f"跳过旧的序列号: {index_entry.sequence_no}")
                # 更新索引位置，继续读取下一条
                self.last_index_size += IndexEntry.size()
                return None  # 跳过旧数据
            
            # 更新索引位置
            self.last_index_size += IndexEntry.size()
            
            # 读取数据头
            header_data = self.data_mmap.read_at(index_entry.offset, MarketDataHeader.size())
            if not header_data or len(header_data) < MarketDataHeader.size():
                self.logger.error("无法读取市场数据头部")
                return None  # 数据不完整
            
            # 解析数据头
            header = MarketDataHeader.from_bytes(header_data)
            if header.body_length <= 0:
                self.logger.warning(f"无效的消息体长度: {header.body_length}")
                return None  # 数据不完整
            
            # 读取数据体
            body_offset = index_entry.offset + MarketDataHeader.size()
            body_data = self.data_mmap.read_at(body_offset, header.body_length)
            if not body_data or len(body_data) < header.body_length:
                self.logger.error(f"无法读取市场数据体: 预期长度={header.body_length}, 实际长度={len(body_data) if body_data else 0}")
                return None  # 数据不完整
            
            # 解析 Protobuf 消息
            envelope = Envelope()
            try:
                envelope.ParseFromString(body_data)
            except Exception as e:
                self.logger.error(f"解析 Protobuf 消息失败: 序列号={header.sequence_no}, 错误={str(e)}")
                return None  # 解析失败
            
            # 更新当前序列号
            self.current_sequence = header.sequence_no
            
            # 更新读取计数并打印进度
            self.read_count += 1
            if self.read_count % 1000 == 0:
                self.logger.debug(f"已读取 {self.read_count} 条市场数据")
            
            return header, envelope
            
        except Exception as e:
            self.logger.error(f"顺序读取行情数据失败: {str(e)}")
            return None

    def close(self):
        """关闭读取器，释放资源"""
        try:
            if self.data_mmap:
                self.data_mmap.close()
                self.data_mmap = None
            
            if self.index_mmap:
                self.index_mmap.close()
                self.index_mmap = None
            
            self.logger.info("行情数据读取器已关闭")
        except Exception as e:
            self.logger.error(f"关闭行情数据读取器异常: {str(e)}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
