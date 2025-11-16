"""
Data models for the l2data_reader package.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .proto.market_data_pb2 import (
    MessageTypeEnum,
    SecuDepthMarketData,
    TransactionEntrustData,
    TransactionTradeData
)

@dataclass
class Snapshot:
    """数据结构，用于保存单个股票的行情快照"""
    symbol: str
    tick: Optional[SecuDepthMarketData] = None
    orders: List[TransactionEntrustData] = field(default_factory=list)
    transactions: List[TransactionTradeData] = field(default_factory=list)

@dataclass
class Slice:
    """数据结构，用于保存多个股票的行情快照"""
    ticks: Dict[str, Snapshot] = field(default_factory=dict)
