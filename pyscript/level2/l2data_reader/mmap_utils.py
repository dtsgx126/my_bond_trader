"""
内存映射文件操作的公共工具模块
提供对内存映射文件的读写操作，支持小文件处理
"""

import os
import mmap
import logging
from typing import Optional, Tuple, Any, BinaryIO

from .utils import get_allocation_granularity

# 获取系统分配粒度
ALLOCATION_GRANULARITY = get_allocation_granularity()

class MemoryMappedFile:
    """
    内存映射文件操作类，提供对文件的内存映射读写功能
    支持小文件处理，确保映射大小为分配粒度的整数倍
    """
    def __init__(self, logger, filename: str, window_size: int = 1024*1024*4, 
                 write_mode: bool = False, create_if_not_exists: bool = False):
        """
        初始化内存映射文件
        
        参数:
            logger: 日志记录器
            filename: 文件路径
            window_size: 映射窗口大小，默认4MB
            write_mode: 是否为写入模式，默认为False（只读模式）
            create_if_not_exists: 如果文件不存在是否创建，默认为False
        """
        self.logger = logger
        self.filename = filename
        self.alignment = ALLOCATION_GRANULARITY
        # 确保窗口大小是分配粒度的整数倍，且至少是分配粒度
        self.window_size = max(
            (window_size // self.alignment) * self.alignment,
            self.alignment
        )
        
        self.write_mode = write_mode
        self.file = None
        self.file_size = 0
        self.current_pos = 0
        self.window_start = 0
        self.mapped_data = None
        
        # 打开或创建文件
        self._open_file(create_if_not_exists)
        
        # 映射窗口
        self._map_window()
    
    def _open_file(self, create_if_not_exists: bool):
        """打开文件，如果需要则创建"""
        mode = 'r+b' if self.write_mode else 'rb'
        
        if not os.path.exists(self.filename):
            if create_if_not_exists:
                # 创建目录
                os.makedirs(os.path.dirname(self.filename), exist_ok=True)
                # 创建空文件并确保其大小为分配粒度的整数倍
                with open(self.filename, 'wb') as f:
                    # 写入一个字节到分配粒度-1的位置，确保文件大小为分配粒度
                    f.seek(self.alignment - 1)
                    f.write(b'\0')
                self.logger.debug(f"创建新文件: {self.filename}, 初始大小={self.alignment}")
            else:
                raise FileNotFoundError(f"文件不存在: {self.filename}")
        
        self.file = open(self.filename, mode)
        self.file_size = os.path.getsize(self.filename)
        
        # 如果是写入模式且文件大小小于分配粒度，则扩展文件
        if self.write_mode and self.file_size < self.alignment:
            self.file.seek(self.alignment - 1)
            self.file.write(b'\0')
            self.file.flush()
            self.file_size = os.path.getsize(self.filename)
            self.logger.debug(f"扩展小文件到分配粒度: {self.filename}, 新大小={self.file_size}")
    
    def _align_offset(self, offset: int) -> Tuple[int, int]:
        """
        对齐偏移量，返回(对齐的起始位置，需要跳过的字节数)
        """
        aligned_start = (offset // self.alignment) * self.alignment
        skip_bytes = offset - aligned_start
        return aligned_start, skip_bytes

    def _map_window(self):
        """映射新的文件窗口，确保偏移量对齐"""
        try:
            if self.mapped_data:
                self.mapped_data.close()
                self.mapped_data = None

            # 获取对齐的起始位置和需要跳过的字节数
            aligned_start, skip_bytes = self._align_offset(self.window_start)
            
            # 计算需要映射的大小（包括对齐补偿）
            mapping_size = self.window_size + skip_bytes
            # 确保映射大小也是分配粒度的整数倍
            mapping_size = ((mapping_size + self.alignment - 1) // self.alignment) * self.alignment
            
            # 处理小文件情况
            if aligned_start + mapping_size > self.file_size:
                # 修改：无论是读取还是写入模式，都尝试扩展文件
                # 计算需要的文件大小
                required_size = aligned_start + mapping_size
                # 确保新大小是分配粒度的整数倍
                required_size = ((required_size + self.alignment - 1) // self.alignment) * self.alignment
                
                if self.write_mode:
                    # 写入模式下，扩展文件
                    self._extend_file(required_size)
                else:
                    # 读取模式下，尝试临时切换到写入模式扩展文件
                    if self.file_size == 0:
                        self.logger.warning(f"文件为空: {self.filename}")
                        return
                    
                    if aligned_start >= self.file_size:
                        self.logger.warning(f"映射起始位置超出文件大小: {aligned_start} >= {self.file_size}")
                        return
                    
                    # 尝试扩展文件
                    try:
                        self.logger.info(f"尝试扩展文件以满足映射需求: {self.filename}, 当前大小={self.file_size}, 需要大小={required_size}")
                        # 关闭当前文件
                        current_pos = self.current_pos
                        if self.file:
                            self.file.close()
                        
                        # 以写入模式重新打开文件
                        self.file = open(self.filename, 'r+b')
                        # 扩展文件
                        self.file.seek(required_size - 1)
                        self.file.write(b'\0')
                        self.file.flush()
                        
                        # 更新文件大小
                        self.file_size = os.path.getsize(self.filename)
                        self.logger.info(f"文件已扩展: {self.filename}, 新大小={self.file_size}")
                        
                        # 恢复文件指针位置
                        self.current_pos = current_pos
                    except Exception as e:
                        self.logger.error(f"扩展文件失败，继续使用原始大小: {self.filename}, {e}")
                        # 如果扩展失败，回退到原来的处理方式
                        # 调整映射大小，确保不超过文件大小
                        mapping_size = min(mapping_size, self.file_size - aligned_start)
                        # 确保映射大小是分配粒度的整数倍
                        mapping_size = (mapping_size // self.alignment) * self.alignment
                        
                        # 如果调整后的映射大小小于分配粒度，则从文件开始处映射一个分配粒度
                        if mapping_size < self.alignment:
                            aligned_start = 0
                            mapping_size = min(self.alignment, self.file_size)
            
            # 如果文件大小为0或映射大小为0，则不进行映射
            if self.file_size == 0 or mapping_size == 0:
                self.logger.debug(f"文件大小为0或映射大小为0，跳过映射: {self.filename}")
                return
            
            # 创建内存映射
            access = mmap.ACCESS_WRITE if self.write_mode else mmap.ACCESS_READ
            self.mapped_data = mmap.mmap(
                self.file.fileno(),
                mapping_size,
                offset=aligned_start,
                access=access
            )
            
            # 更新实际的窗口参数
            self.window_start = aligned_start
            self.window_size = mapping_size
            
            self.logger.debug(f"映射窗口 - 文件: {self.filename}, 起始: {aligned_start}, "
                          f"大小: {mapping_size}, 跳过字节: {skip_bytes}")

        except Exception as e:
            self.logger.error(f"映射文件窗口失败: {self.filename}, 偏移量={self.window_start}, "
                          f"对齐起始位置={aligned_start}, 映射大小={mapping_size}, "
                          f"文件大小={self.file_size}, 错误={str(e)}")
            self.mapped_data = None
            raise
    
    def _extend_file(self, new_size: int):
        """扩展文件到指定大小"""
        try:
            # 确保新大小是分配粒度的整数倍
            new_size = ((new_size + self.alignment - 1) // self.alignment) * self.alignment
            
            # 扩展文件到所需大小
            self.file.seek(new_size - 1)
            self.file.write(b'\0')
            self.file.flush()
            
            # 更新文件大小
            self.file_size = os.path.getsize(self.filename)
            self.logger.debug(f"文件已扩展: {self.filename}, 新大小={self.file_size}")
        except Exception as e:
            self.logger.error(f"扩展文件失败: {self.filename}, {e}")
            raise

    def read(self, size: int) -> Optional[bytes]:
        """读取指定大小的数据"""
        if size <= 0:
            return None

        try:
            # 动态更新文件大小
            self.get_size()

            if self.current_pos >= self.file_size:
                return None

            # 检查是否需要重新映射窗口
            window_offset = self.current_pos - self.window_start
            if (not self.mapped_data or 
                window_offset < 0 or 
                window_offset + size > self.window_size):
                
                # 设置新的窗口起始位置
                self.window_start = (self.current_pos // self.alignment) * self.alignment
                self._map_window()
                window_offset = self.current_pos - self.window_start

            if not self.mapped_data:
                return None

            read_size = min(size, self.file_size - self.current_pos)
            self.mapped_data.seek(window_offset)
            data = self.mapped_data.read(read_size)

            if data:
                self.current_pos += len(data)

            return data

        except Exception as e:
            self.logger.error(f"读取映射文件失败: {self.filename}, 当前位置={self.current_pos}, "
                          f"窗口起始={self.window_start}, 窗口大小={self.window_size}, "
                          f"请求大小={size}, 错误={str(e)}")
            return None

    def write(self, data: bytes) -> bool:
        """写入数据"""
        if not data:
            return True

        try:
            size = len(data)
            
            # 检查是否需要重新映射窗口
            window_offset = self.current_pos - self.window_start
            if (not self.mapped_data or 
                window_offset < 0 or 
                window_offset + size > self.window_size):
                
                # 设置新的窗口起始位置
                self.window_start = (self.current_pos // self.alignment) * self.alignment
                self._map_window()
                window_offset = self.current_pos - self.window_start

            if not self.mapped_data:
                return False

            self.mapped_data.seek(window_offset)
            self.mapped_data.write(data)
            self.current_pos += size
            
            # 更新文件大小（如果写入扩展了文件）
            if self.current_pos > self.file_size:
                self.file_size = self.current_pos

            return True

        except Exception as e:
            self.logger.error(f"写入映射文件失败: {self.filename}, 当前位置={self.current_pos}, "
                          f"窗口起始={self.window_start}, 窗口大小={self.window_size}, "
                          f"数据大小={len(data)}, 错误={str(e)}")
            return False

    def seek(self, pos: int):
        """设置文件指针位置"""
        try:
            # 动态更新文件大小
            self.get_size()
            
            if pos < 0:
                pos = 0
            elif pos > self.file_size:
                pos = self.file_size

            self.current_pos = pos
            
            # 检查新位置是否在当前窗口内
            window_offset = pos - self.window_start
            if (not self.mapped_data or 
                window_offset < 0 or 
                window_offset >= self.window_size):
                
                # 对齐到分配粒度
                self.window_start = (pos // self.alignment) * self.alignment
                self._map_window()
                
        except Exception as e:
            self.logger.error(f"设置文件指针位置失败: {self.filename}, 目标位置={pos}, "
                          f"当前位置={self.current_pos}, 错误={str(e)}")
            raise

    def get_size(self) -> int:
        """获取最新的文件大小"""
        try:
            self.file_size = os.path.getsize(self.filename)
            return self.file_size
        except Exception as e:
            self.logger.error(f"获取文件大小失败: {self.filename}, {str(e)}")
            raise

    def flush(self):
        """刷新内存映射到磁盘"""
        try:
            if self.mapped_data:
                self.mapped_data.flush()
            if self.file:
                self.file.flush()
        except Exception as e:
            self.logger.error(f"刷新文件失败: {self.filename}, {str(e)}")
            raise

    def close(self):
        """关闭文件和映射"""
        try:
            if self.mapped_data:
                self.mapped_data.close()
                self.mapped_data = None
            if self.file:
                self.file.close()
                self.file = None
        except Exception as e:
            self.logger.error(f"关闭文件失败: {self.filename}, {str(e)}")
            raise

    def read_at(self, offset: int, size: int) -> Optional[bytes]:
        """从指定偏移位置读取数据"""
        try:
            # 先定位到指定位置
            self.seek(offset)
            # 然后读取数据
            return self.read(size)
        except Exception as e:
            self.logger.error(f"从指定位置读取数据失败: {self.filename}, 偏移={offset}, 大小={size}, 错误={str(e)}")
            return None
    
    def write_at(self, offset: int, data: bytes) -> bool:
        """在指定偏移位置写入数据"""
        try:
            # 先定位到指定位置
            self.seek(offset)
            # 然后写入数据
            return self.write(data)
        except Exception as e:
            self.logger.error(f"在指定位置写入数据失败: {self.filename}, 偏移={offset}, 数据大小={len(data)}, 错误={str(e)}")
            return False