"""
枚举类型定义，从 market_data.proto 中导出
"""

from enum import Enum, IntEnum

# 从 proto 文件中导入枚举类型
from .proto.market_data_pb2 import (
    MessageTypeEnum,
    TransFlagEnum,
    TrdTypeEnum,
    DirectionEnum,
    OrdActionTypeEnum,
    TickStatusFlagEnum,
    ExerciseStyleEnum,
    HktTradeLimitEnum,
    BondTrdTypeEnum,
    SecurityTypeEnum,
    OptionsTypeEnum,
    InstrumentTradeStatusEnum,
    SettleTypeEnum,
    BondBidExecInstTypeEnum,
    BondBidTransTypeEnum,
    RebuildTransTypeEnum,
    SubSecurityTypeEnum,
    MarketBizTypeEnum,
    InvestorTypeEnum,
)

# 交易标志枚举
class TransFlag(IntEnum):
    """逐笔行情数据标识"""
    UNKNOWN = TransFlagEnum.TRSF_UnKnown  # 未知标识类型
    ALONE = TransFlagEnum.TRSF_Alone      # 逐笔成交与委托序号独立编号
    UNIFIED = TransFlagEnum.TRSF_Unified  # 逐笔成交与委托序号统一编号

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            TransFlag.UNKNOWN: f"未知标识类型({self.name}={char_str})",
            TransFlag.ALONE: f"逐笔独立编号({self.name}={char_str})",
            TransFlag.UNIFIED: f"逐笔统一编号({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class TrdType(IntEnum):
    """交易类型"""
    UNKNOWN = TrdTypeEnum.TRDTRDT_UNKNOWN      # 未知
    BUY = TrdTypeEnum.TRDTRDT_BUY              # 主动买，SH: 内外盘标识('B'的ASCII码)
    SELL = TrdTypeEnum.TRDTRDT_SELL            # 主动卖，SH: 内外盘标识('S'的ASCII码)
    UNKNOWN_N = TrdTypeEnum.TRDTRDT_UNKNOWN_N  # 未知，SH: 内外盘标识('N'的ASCII码)
    CANCEL = TrdTypeEnum.TRDTRDT_CANCEL        # SZ: 成交标识('4'的ASCII码)
    DEAL = TrdTypeEnum.TRDTRDT_DEAL            # SZ: 成交标识('F'的ASCII码)

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            TrdType.UNKNOWN: f"未知({self.name}={char_str})",
            TrdType.BUY: f"主动买({self.name}={char_str})",
            TrdType.SELL: f"主动卖({self.name}={char_str})",
            TrdType.UNKNOWN_N: f"未知N({self.name}={char_str})",
            TrdType.CANCEL: f"撤单({self.name}={char_str})",
            TrdType.DEAL: f"成交({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

# 交易方向枚举
class Direction(IntEnum):
    """交易方向"""
    UNKNOWN = DirectionEnum.TSDIR_UNKNOWN  # 未知
    BUY = DirectionEnum.TSDIR_BUY          # 买单 ('1'的ASCII码)
    SELL = DirectionEnum.TSDIR_SELL        # 卖单 ('2'的ASCII码)
    BORROW = DirectionEnum.TSDIR_BORROW    # 借入 ('G'的ASCII码)
    LOAN = DirectionEnum.TSDIR_LOAN        # 出借 ('F'的ASCII码)

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            Direction.UNKNOWN: f"未知({self.name}={char_str})",
            Direction.BUY: f"买入({self.name}={char_str})",
            Direction.SELL: f"卖出({self.name}={char_str})",
            Direction.BORROW: f"借入({self.name}={char_str})",
            Direction.LOAN: f"出借({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class OrdActionType(IntEnum):
    """订单类别"""
    UNKNOWN = OrdActionTypeEnum.TORT_UNKNOWN        # 未知类型
    MARKET = OrdActionTypeEnum.TORT_Market          # SZ市价 ('1'的ASCII码)
    LIMIT = OrdActionTypeEnum.TORT_Limit            # SZ限价 ('2'的ASCII码)
    MARKET_SELF = OrdActionTypeEnum.TORT_MarketSelf # SZ本方最优 ('U'的ASCII码)
    ADD = OrdActionTypeEnum.TORT_Add                # SH增加委托订单 ('A'的ASCII码)
    DELETE = OrdActionTypeEnum.TORT_Delete          # SH删除委托订单 ('D'的ASCII码)
    STATUS = OrdActionTypeEnum.TORT_Status          # 产品状态订单 ('S'的ASCII码)

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            OrdActionType.UNKNOWN: f"未知({self.name}={char_str})",
            OrdActionType.MARKET: f"市价委托({self.name}={char_str})",
            OrdActionType.LIMIT: f"限价委托({self.name}={char_str})",
            OrdActionType.MARKET_SELF: f"本方最优({self.name}={char_str})",
            OrdActionType.ADD: f"增加订单({self.name}={char_str})",
            OrdActionType.DELETE: f"删除订单({self.name}={char_str})",
            OrdActionType.STATUS: f"产品状态订单({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class TickStatusFlag(IntEnum):
    """行情状态标志"""
    UNKNOWN = TickStatusFlagEnum.TSF_UNKNOWN  # 未知
    ADD = TickStatusFlagEnum.TSF_ADD          # 产品未上市 ('1'的ASCII码)
    START = TickStatusFlagEnum.TSF_START      # 启动 ('2'的ASCII码)
    OCALL = TickStatusFlagEnum.TSF_OCALL      # 开市集合竞价 ('3'的ASCII码)
    TRADE = TickStatusFlagEnum.TSF_TRADE      # 连续自动撮合 ('4'的ASCII码)
    SUSP = TickStatusFlagEnum.TSF_SUSP        # 停牌 ('5'的ASCII码)
    CCALL = TickStatusFlagEnum.TSF_CCALL      # 收盘集合竞价 ('6'的ASCII码)
    CLOSE = TickStatusFlagEnum.TSF_CLOSE      # 闭市 ('7'的ASCII码)
    ENDTR = TickStatusFlagEnum.TSF_ENDTR      # 交易结束 ('8'的ASCII码)

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            TickStatusFlag.UNKNOWN: f"未知({self.name}={char_str})",
            TickStatusFlag.ADD: f"产品未上市({self.name}={char_str})",
            TickStatusFlag.START: f"启动({self.name}={char_str})",
            TickStatusFlag.OCALL: f"开市集合竞价({self.name}={char_str})",
            TickStatusFlag.TRADE: f"连续自动撮合({self.name}={char_str})",
            TickStatusFlag.SUSP: f"停牌({self.name}={char_str})",
            TickStatusFlag.CCALL: f"收盘集合竞价({self.name}={char_str})",
            TickStatusFlag.CLOSE: f"闭市({self.name}={char_str})",
            TickStatusFlag.ENDTR: f"交易结束({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值


# 添加BondTradeType枚举类
class BondTradeType(IntEnum):
    """债券交易方式"""
    UNKNOWN = BondTrdTypeEnum.BTT_UNKNOWN  # 未知类型
    PPCJ = BondTrdTypeEnum.BTT_PPCJ        # 匹配成交
    XSCJ = BondTrdTypeEnum.BTT_XSCJ        # 协商成交
    DJCJ = BondTrdTypeEnum.BTT_DJCJ        # 点击成交
    XJCJ = BondTrdTypeEnum.BTT_XJCJ        # 询价成交
    JMCJ = BondTrdTypeEnum.BTT_JMCJ        # 竞买成交
    YXSB = BondTrdTypeEnum.BTT_YXSB        # 意向申报
    PPDE = BondTrdTypeEnum.BTT_PPDE        # 匹配成交大额
    ZYPPCJ = BondTrdTypeEnum.BTT_ZYPPCJ    # 质押式匹配成交

    def __str__(self):
        return {
            BondTradeType.UNKNOWN: f"未知类型({self.name}={self.value})",
            BondTradeType.PPCJ: f"匹配成交({self.name}={self.value})",
            BondTradeType.XSCJ: f"协商成交({self.name}={self.value})",
            BondTradeType.DJCJ: f"点击成交({self.name}={self.value})",
            BondTradeType.XJCJ: f"询价成交({self.name}={self.value})",
            BondTradeType.JMCJ: f"竞买成交({self.name}={self.value})",
            BondTradeType.YXSB: f"意向申报({self.name}={self.value})",
            BondTradeType.PPDE: f"匹配成交大额({self.name}={self.value})",
            BondTradeType.ZYPPCJ: f"质押式匹配成交({self.name}={self.value})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class InstrumentTradeStatus(IntEnum):
    """合约交易状态"""
    UNKNOWN = 0                                         # 未知
    INIT = InstrumentTradeStatusEnum.INSTS_Init          # 启动(开市前)
    CALL_AUCTION = InstrumentTradeStatusEnum.INSTS_CallAuction  # 集合竞价
    TRINDING = InstrumentTradeStatusEnum.INSTS_Trinding  # 连续交易
    PAUSE = InstrumentTradeStatusEnum.INSTS_Pause        # 休市
    CLOSE = InstrumentTradeStatusEnum.INSTS_Close        # 闭市
    CLOSING_CALL_AUCTION = InstrumentTradeStatusEnum.INSTS_ClosingCallAuction  # 收盘集合竞价
    FUSING = InstrumentTradeStatusEnum.INSTS_Fusing      # 波动性中断
    HALT = InstrumentTradeStatusEnum.INSTS_Halt          # 临时停牌
    HALT_ALL_DAY = InstrumentTradeStatusEnum.INSTS_HaltAllDay  # 全天停牌
    FUSE_TO_CALL_AUCTION = InstrumentTradeStatusEnum.INSTS_FuseToCallAuction  # 熔断(盘中集合竞价)
    FUSE_TO_CLOSE = InstrumentTradeStatusEnum.INSTS_FuseToClose  # 熔断(暂停交易至闭市)
    AFTER_CLOSE_TRADE = InstrumentTradeStatusEnum.INSTS_AfterCloseTrade  # 盘后交易

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            InstrumentTradeStatus.UNKNOWN: f"未知({self.name}={char_str})",
            InstrumentTradeStatus.INIT: f"启动(开市前)({self.name}={char_str})",
            InstrumentTradeStatus.CALL_AUCTION: f"集合竞价({self.name}={char_str})",
            InstrumentTradeStatus.TRINDING: f"连续交易({self.name}={char_str})",
            InstrumentTradeStatus.PAUSE: f"休市({self.name}={char_str})",
            InstrumentTradeStatus.CLOSE: f"闭市({self.name}={char_str})",
            InstrumentTradeStatus.CLOSING_CALL_AUCTION: f"收盘集合竞价({self.name}={char_str})",
            InstrumentTradeStatus.FUSING: f"波动性中断({self.name}={char_str})",
            InstrumentTradeStatus.HALT: f"临时停牌({self.name}={char_str})",
            InstrumentTradeStatus.HALT_ALL_DAY: f"全天停牌({self.name}={char_str})",
            InstrumentTradeStatus.FUSE_TO_CALL_AUCTION: f"熔断(盘中集合竞价)({self.name}={char_str})",
            InstrumentTradeStatus.FUSE_TO_CLOSE: f"熔断(暂停交易至闭市)({self.name}={char_str})",
            InstrumentTradeStatus.AFTER_CLOSE_TRADE: f"盘后交易({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class SecurityType(IntEnum):
    """市场业务类别"""
    UNKNOWN = SecurityTypeEnum.SECT_UnKnown      # 未知类型
    STOCK = SecurityTypeEnum.SECT_Stock          # 股票
    INDEX = SecurityTypeEnum.SECT_Index          # 指数
    FUND = SecurityTypeEnum.SECT_Fund            # 基金
    BOND = SecurityTypeEnum.SECT_Bond            # 债券
    OPTION = SecurityTypeEnum.SECT_Option        # 个股期权
    ETF_OPTION = SecurityTypeEnum.SECT_ETFOption # ETF期权
    FUTU = SecurityTypeEnum.SECT_FUTU            # 期货
    FUTU_OPTION = SecurityTypeEnum.SECT_FUTUOption # 期货期权

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            SecurityType.UNKNOWN: f"未知类型({self.name}={char_str})",
            SecurityType.STOCK: f"股票({self.name}={char_str})",
            SecurityType.INDEX: f"指数({self.name}={char_str})",
            SecurityType.FUND: f"基金({self.name}={char_str})",
            SecurityType.BOND: f"债券({self.name}={char_str})",
            SecurityType.OPTION: f"个股期权({self.name}={char_str})",
            SecurityType.ETF_OPTION: f"ETF期权({self.name}={char_str})",
            SecurityType.FUTU: f"期货({self.name}={char_str})",
            SecurityType.FUTU_OPTION: f"期货期权({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class OptionsType(IntEnum):
    """期权类型"""
    UNKNOWN = 0                                # 未知
    CALL_OPTIONS = OptionsTypeEnum.OPTST_CallOptions  # 看涨（认购）
    PUT_OPTIONS = OptionsTypeEnum.OPTST_PutOptions    # 看跌（认沽）

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            OptionsType.UNKNOWN: f"未知({self.name}={char_str})",
            OptionsType.CALL_OPTIONS: f"看涨(认购)({self.name}={char_str})",
            OptionsType.PUT_OPTIONS: f"看跌(认沽)({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class ExerciseStyle(IntEnum):
    """期权行权方式"""
    UNKNOWN = 0                                        # 未知
    AMERICAN_OPTIONS = ExerciseStyleEnum.EXES_American_Options  # 美式期权
    EUROPEAN_OPTIONS = ExerciseStyleEnum.EXES_European_Options  # 欧式期权

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            ExerciseStyle.UNKNOWN: f"未知({self.name}={char_str})",
            ExerciseStyle.AMERICAN_OPTIONS: f"美式期权({self.name}={char_str})",
            ExerciseStyle.EUROPEAN_OPTIONS: f"欧式期权({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class HktTradeLimit(IntEnum):
    """港股通订单交易限制类型"""
    UNKNOWN = HktTradeLimitEnum.HKTL_UNKNOWN           # 未知
    ORDER_LIMIT = HktTradeLimitEnum.HKTL_ORDER_LIMIT   # 港股通订单限制交易
    ORDER_UNLIMIT = HktTradeLimitEnum.HKTL_ORDER_UNLIMIT # 港股通订单不限制交易

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            HktTradeLimit.UNKNOWN: f"未知({self.name}={char_str})",
            HktTradeLimit.ORDER_LIMIT: f"港股通订单限制交易({self.name}={char_str})",
            HktTradeLimit.ORDER_UNLIMIT: f"港股通订单不限制交易({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class SettleType(IntEnum):
    """结算方式"""
    UNKNOWN = 0                           # 未知
    DBJE = SettleTypeEnum.SETTLTYPE_DBJE   # 多边净额
    ZBQE = SettleTypeEnum.SETTLTYPE_ZBQE   # 逐笔全额

    def __str__(self):
        return {
            SettleType.UNKNOWN: f"未知({self.name}={self.value})",
            SettleType.DBJE: f"多边净额({self.name}={self.value})",
            SettleType.ZBQE: f"逐笔全额({self.name}={self.value})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class BondBidExecInstType(IntEnum):
    """债券竞买成交方式"""
    UNKNOWN = BondBidExecInstTypeEnum.BEIT_UNKNOWN  # 未知类型
    DYZT = BondBidExecInstTypeEnum.BEIT_DYZT        # 单一主体中标
    DZTDYJ = BondBidExecInstTypeEnum.BEIT_DZTDYJ    # 多主体单一价格
    DZTDCJ = BondBidExecInstTypeEnum.BEIT_DZTDCJ    # 多主体多重价格

    def __str__(self):
        return {
            BondBidExecInstType.UNKNOWN: f"未知类型({self.name}={self.value})",
            BondBidExecInstType.DYZT: f"单一主体中标({self.name}={self.value})",
            BondBidExecInstType.DZTDYJ: f"多主体单一价格({self.name}={self.value})",
            BondBidExecInstType.DZTDCJ: f"多主体多重价格({self.name}={self.value})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class BondBidTransType(IntEnum):
    """债券竞买业务类别"""
    UNKNOWN = BondBidTransTypeEnum.BBTT_UNKNOWN  # 未知类型
    YYSB = BondBidTransTypeEnum.BBTT_YYSB        # 竞买预约申报
    FQSB = BondBidTransTypeEnum.BBTT_FQSB        # 竞买发起申报
    YJSB = BondBidTransTypeEnum.BBTT_YJSB        # 竞买应价申报

    def __str__(self):
        return {
            BondBidTransType.UNKNOWN: f"未知类型({self.name}={self.value})",
            BondBidTransType.YYSB: f"竞买预约申报({self.name}={self.value})",
            BondBidTransType.FQSB: f"竞买发起申报({self.name}={self.value})",
            BondBidTransType.YJSB: f"竞买应价申报({self.name}={self.value})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class RebuildTransType(IntEnum):
    """重建返回逐笔类型"""
    UNKNOWN = 0                                  # 未知
    TRADE = RebuildTransTypeEnum.RTT_Trade            # 逐笔成交
    ENTRUST = RebuildTransTypeEnum.RTT_Entrust        # 逐笔委托
    BOND_TRADE = RebuildTransTypeEnum.RTT_BOND_Trade  # 债券逐笔成交
    BOND_ENTRUST = RebuildTransTypeEnum.RTT_BOND_Entrust  # 债券逐笔委托

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            RebuildTransType.UNKNOWN: f"未知({self.name}={char_str})",
            RebuildTransType.TRADE: f"逐笔成交({self.name}={char_str})",
            RebuildTransType.ENTRUST: f"逐笔委托({self.name}={char_str})",
            RebuildTransType.BOND_TRADE: f"债券逐笔成交({self.name}={char_str})",
            RebuildTransType.BOND_ENTRUST: f"债券逐笔委托({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class SubSecurityType(IntEnum):
    """市场业务详细类别"""
    UNKNOWN = SubSecurityTypeEnum.SSET_UnKnown              # 未知类型
    INDEX = SubSecurityTypeEnum.SSET_Index                  # 指数
    S_A = SubSecurityTypeEnum.SSET_S_A                      # 主板A股
    S_B = SubSecurityTypeEnum.SSET_S_B                      # 主板B股
    S_STAR = SubSecurityTypeEnum.SSET_S_Star                # 科创板
    S_GEM = SubSecurityTypeEnum.SSET_S_Gem                  # 创业板
    S_PREFER = SubSecurityTypeEnum.SSET_S_Prefer            # 优先股
    S_OTHER_STOCK = SubSecurityTypeEnum.SSET_S_OtherStock   # 其他股票
    S_ZSZR = SubSecurityTypeEnum.SSET_S_ZSZR                # 做市转让
    S_JJZR = SubSecurityTypeEnum.SSET_S_JJZR                # 竞价转让
    S_LWTS = SubSecurityTypeEnum.SSET_S_LWTS                # 两网及退市
    S_LXJJ = SubSecurityTypeEnum.SSET_S_LXJJ                # 连续竞价(北交所)
    S_QTZR = SubSecurityTypeEnum.SSET_S_QTZR                # 其他转让
    F_ETF = SubSecurityTypeEnum.SSET_F_ETF                  # 交易型开放式指数基金(ETF)
    F_LOF = SubSecurityTypeEnum.SSET_F_LOF                  # 上市开放基金(LOF)
    F_CEF = SubSecurityTypeEnum.SSET_F_CEF                  # 封闭式基金
    F_SF = SubSecurityTypeEnum.SSET_F_SF                    # 分级基金
    F_OEF = SubSecurityTypeEnum.SSET_F_OEF                  # 开放式基金
    F_REITS = SubSecurityTypeEnum.SSET_F_REITs              # 不动产投资信托基金(REITs)
    F_OTHER_FUND = SubSecurityTypeEnum.SSET_F_OtherFund     # 其他基金
    D_GBF = SubSecurityTypeEnum.SSET_D_GBF                  # 国债
    D_CBF = SubSecurityTypeEnum.SSET_D_CBF                  # 企业债
    D_CPF = SubSecurityTypeEnum.SSET_D_CPF                  # 公司债
    D_CCF = SubSecurityTypeEnum.SSET_D_CCF                  # 可转债
    D_REPO = SubSecurityTypeEnum.SSET_D_REPO                # 债券回购
    D_WIT = SubSecurityTypeEnum.SSET_D_WIT                  # 债券预发行
    D_OTHER_BOND = SubSecurityTypeEnum.SSET_D_OtherBond     # 其他债券

    def __str__(self):
        string_value = self._get_string_value()
        return {
            SubSecurityType.UNKNOWN: f"未知类型({self.name}={string_value})",
            SubSecurityType.INDEX: f"指数({self.name}={string_value})",
            SubSecurityType.S_A: f"主板A股({self.name}={string_value})",
            SubSecurityType.S_B: f"主板B股({self.name}={string_value})",
            SubSecurityType.S_STAR: f"科创板({self.name}={string_value})",
            SubSecurityType.S_GEM: f"创业板({self.name}={string_value})",
            SubSecurityType.S_PREFER: f"优先股({self.name}={string_value})",
            SubSecurityType.S_OTHER_STOCK: f"其他股票({self.name}={string_value})",
            SubSecurityType.S_ZSZR: f"做市转让({self.name}={string_value})",
            SubSecurityType.S_JJZR: f"竞价转让({self.name}={string_value})",
            SubSecurityType.S_LWTS: f"两网及退市({self.name}={string_value})",
            SubSecurityType.S_LXJJ: f"连续竞价(北交所)({self.name}={string_value})",
            SubSecurityType.S_QTZR: f"其他转让({self.name}={string_value})",
            SubSecurityType.F_ETF: f"交易型开放式指数基金(ETF)({self.name}={string_value})",
            SubSecurityType.F_LOF: f"上市开放基金(LOF)({self.name}={string_value})",
            SubSecurityType.F_CEF: f"封闭式基金({self.name}={string_value})",
            SubSecurityType.F_SF: f"分级基金({self.name}={string_value})",
            SubSecurityType.F_OEF: f"开放式基金({self.name}={string_value})",
            SubSecurityType.F_REITS: f"不动产投资信托基金(REITs)({self.name}={string_value})",
            SubSecurityType.F_OTHER_FUND: f"其他基金({self.name}={string_value})",
            SubSecurityType.D_GBF: f"国债({self.name}={string_value})",
            SubSecurityType.D_CBF: f"企业债({self.name}={string_value})",
            SubSecurityType.D_CPF: f"公司债({self.name}={string_value})",
            SubSecurityType.D_CCF: f"可转债({self.name}={string_value})",
            SubSecurityType.D_REPO: f"债券回购({self.name}={string_value})",
            SubSecurityType.D_WIT: f"债券预发行({self.name}={string_value})",
            SubSecurityType.D_OTHER_BOND: f"其他债券({self.name}={string_value})"
        }[self]

    def _get_string_value(self):
        """获取字符串值"""
        string_map = {
            self.UNKNOWN: "M",
            self.INDEX: "MRI",
            self.S_A: "ESA.M",
            self.S_B: "ESA.B",
            self.S_STAR: "KSH",
            self.S_GEM: "GEM",
            self.S_PREFER: "ER",
            self.S_OTHER_STOCK: "S",
            self.S_ZSZR: "ZSZR",
            self.S_JJZR: "JJZR",
            self.S_LWTS: "LWTS",
            self.S_LXJJ: "LXJJ",
            self.S_QTZR: "QTZR",
            self.F_ETF: "EM.ETF",
            self.F_LOF: "EM.LOF",
            self.F_CEF: "EM.CEF",
            self.F_SF: "EM.SF",
            self.F_OEF: "EM.OEF",
            self.F_REITS: "CB.RET",
            self.F_OTHER_FUND: "F",
            self.D_GBF: "D.GBF",
            self.D_CBF: "D.CBF",
            self.D_CPF: "D.CPF",
            self.D_CCF: "D.CCF",
            self.D_REPO: "D.REPO",
            self.D_WIT: "D.WIT",
            self.D_OTHER_BOND: "D"
        }
        return string_map.get(self, "M")  # 默认返回未知类型的字符串

    @classmethod
    def to_string(cls, value):
        """将枚举值转换为原始字符串表示"""
        if isinstance(value, cls):
            return value._get_string_value()
        
        try:
            enum_value = cls(value)
            return enum_value._get_string_value()
        except ValueError:
            return "M"  # 默认返回未知类型的字符串

    @classmethod
    def from_string(cls, string_value):
        """从原始字符串表示转换为枚举值"""
        string_map = {
            "M": cls.UNKNOWN,
            "MRI": cls.INDEX,
            "ESA.M": cls.S_A,
            "ESA.B": cls.S_B,
            "KSH": cls.S_STAR,
            "GEM": cls.S_GEM,
            "ER": cls.S_PREFER,
            "S": cls.S_OTHER_STOCK,
            "ZSZR": cls.S_ZSZR,
            "JJZR": cls.S_JJZR,
            "LWTS": cls.S_LWTS,
            "LXJJ": cls.S_LXJJ,
            "QTZR": cls.S_QTZR,
            "EM.ETF": cls.F_ETF,
            "EM.LOF": cls.F_LOF,
            "EM.CEF": cls.F_CEF,
            "EM.SF": cls.F_SF,
            "EM.OEF": cls.F_OEF,
            "CB.RET": cls.F_REITS,
            "F": cls.F_OTHER_FUND,
            "D.GBF": cls.D_GBF,
            "D.CBF": cls.D_CBF,
            "D.CPF": cls.D_CPF,
            "D.CCF": cls.D_CCF,
            "D.REPO": cls.D_REPO,
            "D.WIT": cls.D_WIT,
            "D": cls.D_OTHER_BOND
        }
        return string_map.get(string_value, cls.UNKNOWN)

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class MarketBizType(IntEnum):
    """业务类别"""
    UNKNOWN = MarketBizTypeEnum.MBT_UnKnown  # 未知类型
    STOCK = MarketBizTypeEnum.MBT_Stock      # 现货
    OPTION = MarketBizTypeEnum.MBT_Option    # 期权
    FUTURE = MarketBizTypeEnum.MBT_Future    # 期货

    def __str__(self):
        # 直接将整数值转换为字符
        char_value = chr(self.value) if 32 <= self.value <= 126 else ""
        char_str = f"'{char_value}'" if char_value else ""
        
        return {
            MarketBizType.UNKNOWN: f"未知类型({self.name}={char_str})",
            MarketBizType.STOCK: f"现货({self.name}={char_str})",
            MarketBizType.OPTION: f"期权({self.name}={char_str})",
            MarketBizType.FUTURE: f"期货({self.name}={char_str})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

class InvestorType(IntEnum):
    """债券交易主体类型"""
    UNKNOWN = 0                          # 未知类型
    ZY = InvestorTypeEnum.INT_ZY         # 自营
    ZG = InvestorTypeEnum.INT_ZG         # 资管
    JGJJ = InvestorTypeEnum.INT_JGJJ     # 机构经纪
    GRJJ = InvestorTypeEnum.INT_GRJJ     # 个人经纪

    def __str__(self):
        string_value = self._get_string_value()
        return {
            InvestorType.UNKNOWN: f"未知类型({self.name})",
            InvestorType.ZY: f"自营({self.name}={string_value})",
            InvestorType.ZG: f"资管({self.name}={string_value})",
            InvestorType.JGJJ: f"机构经纪({self.name}={string_value})",
            InvestorType.GRJJ: f"个人经纪({self.name}={string_value})"
        }[self]

    def _get_string_value(self):
        """获取字符串值"""
        string_map = {
            self.UNKNOWN: "",
            self.ZY: "01",
            self.ZG: "02",
            self.JGJJ: "03",
            self.GRJJ: "04"
        }
        return string_map.get(self, "")  # 默认返回空字符串

    @classmethod
    def to_string(cls, value):
        """将枚举值转换为原始字符串表示"""
        if isinstance(value, cls):
            return value._get_string_value()
        
        try:
            enum_value = cls(value)
            return enum_value._get_string_value()
        except ValueError:
            return ""  # 默认返回空字符串

    @classmethod
    def from_string(cls, string_value):
        """从原始字符串表示转换为枚举值"""
        string_map = {
            "": cls.UNKNOWN,
            "01": cls.ZY,
            "02": cls.ZG,
            "03": cls.JGJJ,
            "04": cls.GRJJ
        }
        return string_map.get(string_value, cls.UNKNOWN)

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值

# 添加 MessageType 枚举类
class MessageType(IntEnum):
    """消息类型枚举，用于标识Envelope中包含的具体消息类型"""
    UNKNOWN = MessageTypeEnum.MSG_UNKNOWN                          # 未知消息类型
    SECU_DEPTH_MARKET_DATA = MessageTypeEnum.MSG_SECU_DEPTH_MARKET_DATA        # 现货行情
    TRANSACTION_ENTRUST = MessageTypeEnum.MSG_TRANSACTION_ENTRUST           # 逐笔委托数据
    TRANSACTION_TRADE = MessageTypeEnum.MSG_TRANSACTION_TRADE             # 逐笔成交数据
    SUBSCRIBE_OK = MessageTypeEnum.MSG_SUBSCRIBE_OK                  # 行情订阅定成通知
    FUTU_DEPTH_MARKET_DATA = MessageTypeEnum.MSG_FUTU_DEPTH_MARKET_DATA        # 期货行情
    FUTU_INSTRUMENT_STATIC_INFO = MessageTypeEnum.MSG_FUTU_INSTRUMENT_STATIC_INFO   # 期货静态行情
    SECU_INSTRUMENT_STATIC_INFO = MessageTypeEnum.MSG_SECU_INSTRUMENT_STATIC_INFO   # 现货静态行情
    OPT_DEPTH_MARKET_DATA = MessageTypeEnum.MSG_OPT_DEPTH_MARKET_DATA         # 期权行情
    OPT_INSTRUMENT_STATIC_INFO = MessageTypeEnum.MSG_OPT_INSTRUMENT_STATIC_INFO    # 期权静态行情
    HKT_DEPTH_MARKET_DATA = MessageTypeEnum.MSG_HKT_DEPTH_MARKET_DATA         # 港股行情
    HKT_INSTRUMENT_STATIC_INFO = MessageTypeEnum.MSG_HKT_INSTRUMENT_STATIC_INFO    # 港股静态行情
    BOND_TRANSACTION_TRADE = MessageTypeEnum.MSG_BOND_TRANSACTION_TRADE        # 债券逐笔成交
    BOND_TRANSACTION_ENTRUST = MessageTypeEnum.MSG_BOND_TRANSACTION_ENTRUST      # 债券逐笔委托
    SECU_DEPTH_MARKET_DATA_PLUS = MessageTypeEnum.MSG_SECU_DEPTH_MARKET_DATA_PLUS   # 现货增强行情

    def __str__(self):
        return {
            MessageType.UNKNOWN: f"未知消息类型({self.name}={self.value})",
            MessageType.SECU_DEPTH_MARKET_DATA: f"现货行情({self.name}={self.value})",
            MessageType.TRANSACTION_ENTRUST: f"逐笔委托数据({self.name}={self.value})",
            MessageType.TRANSACTION_TRADE: f"逐笔成交数据({self.name}={self.value})",
            MessageType.SUBSCRIBE_OK: f"行情订阅成功通知({self.name}={self.value})",
            MessageType.FUTU_DEPTH_MARKET_DATA: f"期货行情({self.name}={self.value})",
            MessageType.FUTU_INSTRUMENT_STATIC_INFO: f"期货静态行情({self.name}={self.value})",
            MessageType.SECU_INSTRUMENT_STATIC_INFO: f"现货静态行情({self.name}={self.value})",
            MessageType.OPT_DEPTH_MARKET_DATA: f"期权行情({self.name}={self.value})",
            MessageType.OPT_INSTRUMENT_STATIC_INFO: f"期权静态行情({self.name}={self.value})",
            MessageType.HKT_DEPTH_MARKET_DATA: f"港股行情({self.name}={self.value})",
            MessageType.HKT_INSTRUMENT_STATIC_INFO: f"港股静态行情({self.name}={self.value})",
            MessageType.BOND_TRANSACTION_TRADE: f"债券逐笔成交({self.name}={self.value})",
            MessageType.BOND_TRANSACTION_ENTRUST: f"债券逐笔委托({self.name}={self.value})",
            MessageType.SECU_DEPTH_MARKET_DATA_PLUS: f"现货增强行情({self.name}={self.value})"
        }[self]

    @classmethod
    def _missing_(cls, value):
        """处理无效的枚举值"""
        return cls.UNKNOWN  # 返回默认值
