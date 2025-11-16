"""
Exceptions for the l2data_reader package.
"""

class NoDataException(Exception):
    """当无法读取到数据时抛出的异常"""
    def __init__(self, message="No market data available"):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return self.message

class DataFormatException(Exception):
    """当数据格式错误时抛出的异常"""
    pass

class FileAccessException(Exception):
    """当文件访问出错时抛出的异常"""
    pass

class IndexOutOfRangeException(Exception):
    """当索引超出范围时抛出的异常"""
    pass

class InvalidArgumentException(Exception):
    """当参数无效时抛出的异常"""
    pass

class ProtobufParseException(Exception):
    """当 Protobuf 解析失败时抛出的异常"""
    pass