"""
L2Data Reader - A package for reading level 2 market data.
"""

__version__ = '0.1.0'

from .reader import MarketDataReader, MarketDataHeader, IndexEntry, MarketDataResult
from .models import Snapshot, Slice
from .exceptions import NoDataException, DataFormatException
from .utils import time_to_milliseconds

# 导入 proto 文件中的主要类结构
from .proto.market_data_pb2 import (
    Envelope,
    SecuDepthMarketData,
    TransactionEntrustData,
    TransactionTradeData,
    FutuDepthMarketData,
    FutuInstrumentStaticInfo,
    SecuDepthMarketDataPlus,
    SecuInstrumentStaticInfo,
    OptInstrumentStaticInfo,
    BondTradeInfo,
    OptDepthMarketData,
    HktInstrumentStaticInfo,
    HktDepthMarketData,
    BondTransactionTradeData,
    BondTransactionEntrustData,
    RtnMarketCloseField,
    RtnMarketOpenField,
    RtnSubscriptionSuccessField
)

# 导出枚举类型
from .enums import (
    MessageType,
    TransFlag, TrdType, Direction, OrdActionType, TickStatusFlag,
    ExerciseStyle, HktTradeLimit, BondTradeType, SecurityType,
    OptionsType, InstrumentTradeStatus, SettleType,
    BondBidExecInstType, BondBidTransType, RebuildTransType,
    SubSecurityType, MarketBizType, InvestorType
)

__all__ = [
    'MarketDataReader',
    'MarketDataHeader',
    'IndexEntry',
    'Snapshot',
    'Slice',
    'MarketDataResult',
    
    # 异常类
    'NoDataException',
    'DataFormatException',
    
    # Proto 消息类型
    'Envelope',
    'SecuDepthMarketData',
    'TransactionEntrustData',
    'TransactionTradeData',
    'FutuDepthMarketData',
    'FutuInstrumentStaticInfo',
    'SecuDepthMarketDataPlus',
    'SecuInstrumentStaticInfo',
    'OptInstrumentStaticInfo',
    'BondTradeInfo',
    'OptDepthMarketData',
    'HktInstrumentStaticInfo',
    'HktDepthMarketData',
    'BondTransactionTradeData',
    'BondTransactionEntrustData',
    'RtnMarketCloseField',
    'RtnMarketOpenField',
    'RtnSubscriptionSuccessField',
    
    # 枚举类型
    'TransFlag',
    'TrdType',
    'Direction',
    'OrdActionType',
    'TickStatusFlag',
    'ExerciseStyle',
    'HktTradeLimit',
    'BondTradeType',
    'SecurityType',
    'OptionsType',
    'InstrumentTradeStatus',
    'SettleType',
    'BondBidExecInstType',
    'BondBidTransType',
    'RebuildTransType',
    'SubSecurityType',
    'MarketBizType',
    'InvestorType',
    
    # 辅助函数
    'time_to_milliseconds',

    # 新增的 MarketDataWriter
    'MarketDataWriter'
]