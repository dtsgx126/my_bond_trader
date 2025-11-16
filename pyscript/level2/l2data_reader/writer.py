
import csv
import os
import struct
import logging
from datetime import datetime

from .proto.market_data_pb2 import (
    Envelope,
    MessageTypeEnum,
    InstrumentTradeStatusEnum,
    SecuDepthMarketData,
    TransactionEntrustData,
    TransactionTradeData
)
from .enums import TransFlag, MessageType

def is_bond(symbol: str) -> bool:
    """
    根据证券代码判断是否为债券
    
    参数:
        symbol: 证券代码，6位数字字符串
        
    返回:
        bool: True表示是债券，False表示是股票或其他证券
    """
    if not symbol or len(symbol.strip()) < 2:
        return False
    
    # 去除可能的空格和交易所后缀
    clean_symbol = symbol.strip().split('.')[0]
    
    # 债券代码规则
    if clean_symbol.startswith(('10', '11', '12', '13')):  # 国债、金融债、企业债、公司债
        return True
    elif clean_symbol.startswith(('110', '123')):  # 可转债
        return True
    elif clean_symbol.startswith('17'):  # 地方政府债
        return True
    elif clean_symbol.startswith('04'):  # 短期融资券
        return True
    
    # 股票代码规则
    if clean_symbol.startswith(('600', '601', '603', '605', '688')):  # 上交所股票
        return False
    elif clean_symbol.startswith(('000', '001', '002', '003', '300')):  # 深交所股票
        return False
    elif clean_symbol.startswith(('8', '4')):  # 北交所股票
        return False
    
    # 其他情况，可以根据需要扩展
    # 默认返回False，即假设为股票
    return False

def convert_balance(balance: str, symbol: str) -> int:
    """
    区分债券和股票的 Balance 转换函数
    债券的 Balance 代表手数, 股票的 Balance 代表股数
    """
    try:
        val = float(balance)
    except ValueError:
        val = 0
    
    # 判断是否为债券，债券和股票的转换逻辑可能不同
    if is_bond(symbol):
        # 债券通常以手为单位，一手=10张
        return int(val * 10)  # 假设债券需要乘以10转换为张数
    else:
        # 股票直接返回股数
        return int(val)

def convert_tick_status(code: str) -> str:
    """
    根据输入的交易状态代码（例如 "ADD", "START", "OCALL", "TRADE", "SUSP", "CLOSE", "ENDTR"），
    返回转换后的枚举行字符串。

    注意：
    - TSF_UNKNOWN 为默认未知状态，值为 0。
    - 对于“CLOSE”项，枚举值为 55，因为在 SUSP 之后应插入 TSF_CCALL（值为 54）。
    - 同时也允许输入 "CCALL"（代表收盘集合竞价），其值为 54。

    使用示例:
        >> print(convert_tick_status("ADD"))
        TSF_ADD = 49; // 产品未上市 ('1'的ASCII码)
    """
    # 定义映射表：映射输入代码到描述和对应的ASCII值
    mapping = {
        "B":      49, # ("买单",           49),  # ASCII '1'
        "S":      50, # ("卖单",           50),  # ASCII '2'
        #"ADD":    49, # ("产品未上市",      49),  # ASCII '1'
        #"START":  50, # ("启动",           50),  # ASCII '2'
        "OCALL":  51, # ("开市集合竞价",    51),  # ASCII '3'
        "TRADE":  52, # ("连续自动撮合",    52),  # ASCII '4'
        "SUSP":   53, # ("停牌",           53),  # ASCII '5'
        "CCALL":  54, # ("收盘集合竞价",    54),  # ASCII '6'
        "CLOSE":  55, # ("闭市",           55),  # ASCII '7'
        "ENDTR":  56, # ("交易结束",        56),  # ASCII '8'
    }
    
    key = code.upper()
    if key in mapping:
        ascii_val = mapping[key]
        return ascii_val
    else:
        return 0 # "TSF_UNKNOWN = 0; // 未知"

def convert_instrument_status(status_code: str) -> int:
    """
    根据输入的合约交易状态代码，返回对应的 InstrumentTradeStatusEnum 枚举值。
    
    参考 HSDataType.h 中的 HSInstrumentTradeStatus 定义：
    - INSTS_Init('S'): 启动(开市前)
    - INSTS_CallAuction('C'): 集合竞价
    - INSTS_Trinding('T'): 连续交易
    - INSTS_Pause('B'): 休市
    - INSTS_Close('E'): 闭市
    - INSTS_ClosingCallAuction('U'): 收盘集合竞价
    - INSTS_Fusing('V'): 波动性中断
    - INSTS_Halt('P'): 临时停牌
    - INSTS_HaltAllDay('1'): 全天停牌
    - INSTS_FuseToCallAuction('M'): 熔断(盘中集合竞价)
    - INSTS_FuseToClose('N'): 熔断(暂停交易至闭市)
    - INSTS_AfterCloseTrade('A'): 盘后交易
    
    使用示例:
        >> print(convert_instrument_status("T"))
        2  # InstrumentTradeStatusEnum.ITS_CONTINUOUS
    """
    # 定义映射表：映射输入代码到 InstrumentTradeStatusEnum 枚举值
    mapping = {
        # 启动(开市前)
        "S": InstrumentTradeStatusEnum.INSTS_Init,  # 1
        # 集合竞价
        "C": InstrumentTradeStatusEnum.INSTS_CallAuction,  # 3
        # 连续交易
        "T": InstrumentTradeStatusEnum.INSTS_Trinding,  # 2
        # 休市
        "B": InstrumentTradeStatusEnum.INSTS_Pause,  # 4
        # 闭市
        "E": InstrumentTradeStatusEnum.INSTS_Close,  # 5
        # 收盘集合竞价
        "U": InstrumentTradeStatusEnum.INSTS_ClosingCallAuction,  # 6
        # 波动性中断
        "V": InstrumentTradeStatusEnum.INSTS_Fusing,  # 7
        # 临时停牌
        "P": InstrumentTradeStatusEnum.INSTS_Halt,  # 8
        # 全天停牌
        "1": InstrumentTradeStatusEnum.INSTS_HaltAllDay,  # 9
        # 熔断(盘中集合竞价)
        "M": InstrumentTradeStatusEnum.INSTS_FuseToCallAuction,  # 10
        # 熔断(暂停交易至闭市)
        "N": InstrumentTradeStatusEnum.INSTS_FuseToClose,  # 11
        # 盘后交易
        "A": InstrumentTradeStatusEnum.INSTS_AfterCloseTrade,  # 12
    }
    
    # 兼容性处理：将常见的交易状态代码映射到对应的字符
    status_mapping = {
        "START": "S",      # 启动 -> 'S'
        "OCALL": "C",      # 开市集合竞价 -> 'C'
        "TRADE": "T",      # 连续交易 -> 'T'
        "PAUSE": "B",      # 休市 -> 'B'
        "CLOSE": "E",      # 闭市 -> 'E'
        "CCALL": "U",      # 收盘集合竞价 -> 'U'
        "SUSP": "P",       # 临时停牌 -> 'P'
        "HALT": "P",       # 临时停牌 -> 'P'
        "HALTALL": "1",    # 全天停牌 -> '1'
        "AFTER": "A",      # 盘后交易 -> 'A'
    }
    
    # 先尝试将输入的代码转换为标准字符
    key = status_code.upper()
    if key in status_mapping:
        key = status_mapping[key]
    
    # 查找对应的枚举值
    if key in mapping:
        return mapping[key]
    else:
        return InstrumentTradeStatusEnum.INSTS_UNKNOWN  # 0，未知状态
    
def get_exchange_suffix(stock_code: str) -> str:
    """根据股票代码判断交易所后缀"""
    if stock_code.startswith('SSE'):
        return '.SH'
    else:
        return '.SZ'

def convert_trading_time(trading_time: str) -> int:
    """
    将 TradingTime 转换为 HHMMSSzzz 格式的整数时间
    支持格式例如:
    - "09:30:00" 或 "09:30:00.123"
    - "2022-10-10 09:15:00.000" (会提取空格后面的时间部分)
    """
    # 如果包含空格，取空格后面的部分
    if ' ' in trading_time:
        trading_time = trading_time.split(' ', 1)[1]

    if '.' in trading_time:
        main, frac = trading_time.split('.', 1)
    else:
        main, frac = trading_time, "000"
    
    # 去除时间中的冒号
    main = main.replace(':', '')
    
    frac = (frac + "000")[:3]  # 确保3位毫秒
    
    # 组合时间和毫秒
    time_int = int(main + frac)

    return time_int  # 例如 "093000123" 转换为整数

def convert_balance(balance: str, symbol: str) -> int:
    """
    区分债券和股票的 Balance 转换函数
    债券的 Balance 代表手数, 股票的 Balance 代表股数
    此处仅做简单转换，实际逻辑可根据 symbol 判断
    """
    try:
        val = float(balance)
    except ValueError:
        val = 0
    return int(val)

class MarketDataWriter:
    """
    MarketDataWriter 用于从 CSV 文件中读取市场数据，
    转换为对应的 Protobuf 消息并以二进制格式写入文件。
    
    支持处理三类 CSV 数据：
      – ORDER：逐笔委托 -> TransactionEntrustData  
      – TRADE：逐笔成交 -> TransactionTradeData  
      – TAQ：tick 数据 -> SecuDepthMarketData  
      
    三个目录中同名文件代表同一股票数据，程序将同时读取这三类 CSV，
    根据 CSV 中 “UNIX” 时间戳字段排序（order 和 trade 内部按 RecNO 顺序排列），
    并生成 bin 数据文件、索引文件（每条记录 {seq_no, offset}）以及包含记录总数的头文件（8字节整数）。
    """
    def __init__(self, logger: logging.Logger, order_csv: str, trade_csv: str, tick_csv: str, static_csv: str, output_dir: str):
        self.logger = logger
        self.order_csv = order_csv
        self.trade_csv = trade_csv
        self.tick_csv = tick_csv
        self.output_dir = output_dir
        
        base_name = os.path.splitext(os.path.basename(order_csv))[0]
        self.is_bond = is_bond(base_name)
        self.full_symbol = f"{base_name}{get_exchange_suffix(base_name)}"
        self.bin_file = os.path.join(output_dir, f"{base_name}.bin")
        self.idx_file = os.path.join(output_dir, f"{base_name}.idx")
        self.hdr_file = os.path.join(output_dir, f"{base_name}.hdr")
        
        # 读取静态信息
        self.static_info = self._load_static_info(base_name, static_csv)
        
        self.sequence_counter = 0  # 用于 tick 数据生成序号
        self.records = []  # 存储待写入的所有记录

    def _load_static_info(self, base_name: str, static_csv: str):
        """
        从STATIC目录下读取相同symbol的所有静态信息
        
        参数:
            static_csv: 静态信息的 CSV 文件路径
        
        返回:
            dict: 包含处理好的静态信息的字典
        """
        # 初始化静态信息字典，设置默认值
        static_info = {
            # 基本信息
            'symbol': base_name,                # 证券代码
            'security_name': '',                # 证券名称
            'security_en': '',                  # 英文名称
            'isin_code': '',                    # ISIN代码
            'symbol_underlying': '',            # 基础证券代码
            'market': '',                       # 市场代码
            'market_type': 0,                   # 市场类型
            'cfi_code': '',                     # CFI代码
            'security_sub_type': '',            # 证券子类型
            'currency': '',                     # 币种
            
            # 交易相关
            'par_value': 0.0,                   # 面值
            'tradable_no': 0,                   # 可交易数量
            'end_date': 0,                      # 到期日
            'listing_date': 0,                  # 上市日期
            'set_no': 0,                        # 集合竞价编号
            'buy_volume_unit': 0,               # 买入数量单位
            'sell_volume_unit': 0,              # 卖出数量单位
            'declare_volume_floor': 0,          # 申报数量下限
            'declare_volume_ceiling': 0,        # 申报数量上限
            
            # 价格相关
            'pre_close_price': 0.0,             # 昨收价
            'tick_size': 0.0,                   # 最小变动价位
            'up_down_limit_type': 0,            # 涨跌幅限制类型
            'price_up_limit': 0.0,              # 涨停价
            'price_down_limit': 0.0,            # 跌停价
            'xr_ratio': 0.0,                    # 除权比例
            'xd_amount': 0.0,                   # 除息金额
            
            # 其他信息
            'crd_buy_underlying': '',           # 融资标的标志
            'security_status': 0,               # 证券状态
            'sample_no': 0,                     # 样本数量
            'sample_avg_price': 0.0,            # 样本平均价
            'trade_amount': 0.0,                # 成交金额
            'avg_capital': 0.0,                 # 平均股本
            'total_market_value': 0.0,          # 总市值
            'market_value_ratio': 0.0,          # 市值比例
            'static_pe_ratio': 0.0,             # 静态市盈率
            'index_level_status': 0,            # 指数级别状态
            'security_id': '',                  # 证券ID
        }
        
        if os.path.exists(static_csv):
            try:
                with open(static_csv, newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    found_matching_symbol = False
                    
                    for row in reader:
                        # 检查Symbol是否匹配
                        row_symbol = row.get('Symbol', '').strip()
                        if row_symbol != base_name:
                            continue
                        found_matching_symbol = True
                        # 只需要读取第一行数据
                        # 基本信息
                        static_info['symbol'] = row.get('Symbol', base_name).strip()
                        static_info['security_name'] = row.get('SecurityName', '').strip()
                        static_info['security_en'] = row.get('SecurityEN', '').strip()
                        static_info['isin_code'] = row.get('ISINCode', '').strip()
                        static_info['symbol_underlying'] = row.get('SymbolUnderlying', '').strip()
                        static_info['market'] = row.get('Market', '').strip()
                        static_info['market_type'] = self._safe_int(row.get('MarketType', '0'))
                        static_info['cfi_code'] = row.get('CFICode', '').strip()
                        static_info['security_sub_type'] = row.get('SecuritySubType', '').strip()
                        static_info['currency'] = row.get('Currency', '').strip()
                        
                        # 交易相关
                        static_info['par_value'] = self._safe_float(row.get('ParValue', '0'))
                        static_info['tradable_no'] = self._safe_int(row.get('TradableNo', '0'))
                        static_info['end_date'] = self._safe_int(row.get('EndDate', '0'))
                        static_info['listing_date'] = self._safe_int(row.get('ListingDate', '0'))
                        static_info['set_no'] = self._safe_int(row.get('SetNo', '0'))
                        static_info['buy_volume_unit'] = self._safe_int(row.get('BuyVolumeUnit', '0'))
                        static_info['sell_volume_unit'] = self._safe_int(row.get('SellVolumeUnit', '0'))
                        static_info['declare_volume_floor'] = self._safe_int(row.get('DeclareVolumeFloor', '0'))
                        static_info['declare_volume_ceiling'] = self._safe_int(row.get('DeclareVolumeCeiling', '0'))
                        
                        # 价格相关
                        static_info['pre_close_price'] = self._safe_float(row.get('PreClosePrice', '0'))
                        static_info['tick_size'] = self._safe_float(row.get('TickSize', '0'))
                        static_info['up_down_limit_type'] = self._safe_int(row.get('UpDownLimitType', '0'))
                        static_info['price_up_limit'] = self._safe_float(row.get('PriceUpLimit', '0'))
                        static_info['price_down_limit'] = self._safe_float(row.get('PriceDownLimit', '0'))
                        static_info['xr_ratio'] = self._safe_float(row.get('XRRatio', '0'))
                        static_info['xd_amount'] = self._safe_float(row.get('XDAmount', '0'))
                        
                        # 其他信息
                        static_info['crd_buy_underlying'] = row.get('CrdBuyUnderlying', '').strip()
                        static_info['security_status'] = self._safe_int(row.get('SecurityStatus', '0'))
                        static_info['sample_no'] = self._safe_int(row.get('SampleNo', '0'))
                        static_info['sample_avg_price'] = self._safe_float(row.get('SampleAvgPrice', '0'))
                        static_info['trade_amount'] = self._safe_float(row.get('TradeAmount', '0'))
                        static_info['avg_capital'] = self._safe_float(row.get('AvgCapital', '0'))
                        static_info['total_market_value'] = self._safe_float(row.get('TotalMarketValue', '0'))
                        static_info['market_value_ratio'] = self._safe_float(row.get('MarketValueRatio', '0'))
                        static_info['static_pe_ratio'] = self._safe_float(row.get('StaticPERatio', '0'))
                        static_info['index_level_status'] = self._safe_int(row.get('IndexLevelStatus', '0'))
                        static_info['security_id'] = row.get('SecurityID', '').strip()
                        
                        self.logger.info(f"从STATIC目录读取到静态信息: {static_info['symbol']}, 证券名称: {static_info['security_name']}, 涨停价: {static_info['price_up_limit']}, 跌停价: {static_info['price_down_limit']}")
                        break  # 只读取第一行
                
                if not found_matching_symbol:
                    self.logger.warning(f"在静态信息文件中未找到匹配的证券代码: {base_name}，将使用默认静态信息")
            except Exception as e:
                self.logger.warning(f"读取STATIC/{base_name}.csv文件失败: {str(e)}")
        else:
            self.logger.warning(f"STATIC/{base_name}.csv文件不存在，将使用默认静态信息")
        
        return static_info
    
    def _safe_float(self, value, default=0.0):
        """安全地将字符串转换为浮点数"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def _safe_int(self, value, default=0):
        """安全地将字符串转换为整数"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def read_csv_files(self):
        """读取并解析三个 CSV 文件，合并为一个排序后的数据列表"""
        orders = []
        with open(self.order_csv, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['data_type'] = 'order'
                try:
                    row['UNIX'] = int(row.get('UNIX', '0'))
                except:
                    row['UNIX'] = 0
                try:
                    row['RecNO'] = int(row.get('RecNO', '0'))
                except:
                    row['RecNO'] = 0
                orders.append(row)
        
        trades = []
        with open(self.trade_csv, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['data_type'] = 'trade'
                try:
                    row['UNIX'] = int(row.get('UNIX', '0'))
                except:
                    row['UNIX'] = 0
                try:
                    row['RecNO'] = int(row.get('RecNO', '0'))
                except:
                    row['RecNO'] = 0
                trades.append(row)
        
        ticks = []
        with open(self.tick_csv, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['data_type'] = 'tick'
                try:
                    row['UNIX'] = int(row.get('UNIX', '0'))
                except:
                    row['UNIX'] = 0
                ticks.append(row)
        
        all_records = orders + trades + ticks
        
        def sort_key(item):
            if item['data_type'] in ['order', 'trade']:
                return (item['UNIX'], item.get('RecNO', 0))
            else:
                return (item['UNIX'], 0)
        
        all_records.sort(key=sort_key)
        self.records = all_records
        self.logger.info(f"读取到 {len(orders)} 条委托数据, {len(trades)} 条成交数据, {len(ticks)} 条 tick 数据。")
    
    def write_bin_files(self):
        """将排序后的记录写入 bin 文件，并生成索引文件和头文件"""
        bin_f = open(self.bin_file, 'wb')
        idx_f = open(self.idx_file, 'wb')
        
        current_offset = 0
        record_count = 0
        
        for rec in self.records:
            envelope = Envelope()
            msg_type = 0
            sequence_no = 0
            
            if rec['data_type'] == 'order':
                msg_type = MessageTypeEnum.MSG_TRANSACTION_ENTRUST  # 逐笔委托消息类型
                sequence_no = int(rec.get('RecID', 0))
                data_msg = TransactionEntrustData()
                data_msg.symbol = self.full_symbol
                data_msg.trans_flag = TransFlag.UNIFIED
                data_msg.seq_no = sequence_no
                data_msg.trade_date = int(rec.get('TradingDate', '').strip())
                data_msg.transact_time = convert_trading_time(rec.get('TradingTime', '00:00:00.000'))
                data_msg.channel_no = 0 # int(rec.get('SetID', '').strip())
                try:
                    data_msg.order_price = float(rec.get('OrderPrice', '0'))
                except:
                    data_msg.order_price = 0.0
                data_msg.order_type = ord(rec.get('OrderType', '').strip()[0:1])
                order_code = rec.get('OrderCode', '').strip()
                data_msg.order_side = convert_tick_status(order_code)
                data_msg.tick_status = convert_tick_status(order_code)
                data_msg.order_id = int(rec.get('OrderID', '').strip())
                data_msg.order_volume = round(float(rec.get('Balance', '0')))
                data_msg.biz_index = rec.get('RecNO', 0)
                data_msg.trade_volume = 0
                data_msg.is_warmup = False
                envelope.transaction_entrust_data.CopyFrom(data_msg)
            
            elif rec['data_type'] == 'trade':
                msg_type = MessageTypeEnum.MSG_TRANSACTION_TRADE  # 逐笔成交消息类型
                sequence_no = int(rec.get('RecID', 0))
                data_msg = TransactionTradeData()
                data_msg.symbol = self.full_symbol
                data_msg.trans_flag = TransFlag.UNIFIED
                data_msg.seq_no = sequence_no
                data_msg.trade_date = int(rec.get('TradingDate', '').strip())
                data_msg.transact_time = convert_trading_time(rec.get('TradingTime', '00:00:00'))
                data_msg.channel_no = 0 # rec.get('SetID', 0)
                try:
                    data_msg.trade_price = float(rec.get('TradePrice', '0'))
                except:
                    data_msg.trade_price = 0.0
                try:
                    data_msg.trade_volume = int(rec.get('TradeVolume', '0'))
                except:
                    data_msg.trade_volume = 0
                try:
                    data_msg.trade_money = float(rec.get('TradeAmount', '0'))
                except:
                    data_msg.trade_money = 0.0
                data_msg.trade_buy_no = int(rec.get('BuyRecID', '').strip())
                data_msg.trade_sell_no = int(rec.get('SellRecID', '').strip())
                data_msg.trade_type = ord(rec.get('BuySellFlag', '').strip())
                data_msg.biz_index = rec.get('RecNO', 0)
                data_msg.is_warmup = False
                envelope.transaction_trade_data.CopyFrom(data_msg)
            
            elif rec['data_type'] == 'tick':
                # 处理 TAQ 目录下的 tick 数据
                msg_type = MessageTypeEnum.MSG_SECU_DEPTH_MARKET_DATA  # tick 数据消息类型
                self.sequence_counter += 1
                sequence_no = self.sequence_counter
                data_msg = SecuDepthMarketData()
                
                # 基本信息
                data_msg.symbol = self.full_symbol
                
                # 交易日期和时间
                data_msg.trade_date = int(rec.get('TradingDate', '').strip())
                data_msg.update_time = convert_trading_time(rec.get('TradingTime', '00:00:00'))
                
                # 交易状态
                trade_status = rec.get('TradeStatus', '').strip()
                if trade_status:
                    data_msg.instrument_trade_status = convert_instrument_status(trade_status)
                
                # 价格信息
                try:
                    data_msg.pre_close_price = float(rec.get('PreClosePrice', '0'))
                except:
                    data_msg.pre_close_price = 0.0
                
                try:
                    data_msg.open_price = float(rec.get('OpenPrice', '0'))
                except:
                    data_msg.open_price = 0.0
                
                try:
                    data_msg.high_price = float(rec.get('HighPrice', '0'))
                except:
                    data_msg.high_price = 0.0
                
                try:
                    data_msg.low_price = float(rec.get('LowPrice', '0'))
                except:
                    data_msg.low_price = 0.0
                
                try:
                    data_msg.last_price = float(rec.get('LastPrice', '0'))
                except:
                    data_msg.last_price = 0.0
                
                # 今收盘，如果获取失败，则取最后成交价。默认为0。
                try:
                    data_msg.close_price = float(rec.get('ClosePrice', '0'))
                except:
                    data_msg.close_price = data_msg.last_price
                
                # 添加价格上下限
                data_msg.upper_limit_price = self.static_info['price_up_limit']
                data_msg.lower_limit_price = self.static_info['price_down_limit']        
                
                try:
                    data_msg.trade_balance = float(rec.get('TotalAmount', '0'))
                except:
                    data_msg.trade_balance = 0.0
                
                # 委托买卖量
                try:
                    data_msg.total_bid_volume = int(rec.get('TotalBuyOrderVolume', '0'))
                except:
                    data_msg.total_bid_volume = 0
                
                try:
                    data_msg.total_ask_volume = int(rec.get('TotalSellOrderVolume', '0'))
                except:
                    data_msg.total_ask_volume = 0
                
                # 加权平均价
                try:
                    data_msg.ma_bid_price = float(rec.get('BondWtAvgBuyPrice', '0')) if self.is_bond else float(rec.get('WtAvgBuyPrice', '0'))
                except:
                    data_msg.ma_bid_price = 0.0
                
                try:
                    data_msg.ma_ask_price = float(rec.get('BondWtAvgBuyPrice', '0')) if self.is_bond else float(rec.get('WtAvgSellPrice', '0'))
                except:
                    data_msg.ma_ask_price = 0.0
                
                # 成交笔数
                try:
                    data_msg.trades_num = int(rec.get('TotalNo', '0'))
                except:
                    data_msg.trades_num = 0.0
                
                # IOPV 净值
                try:
                    data_msg.iopv = float(rec.get('IOPV', '0'))
                except:
                    data_msg.iopv = 0.0
                
                # 债券收益率
                try:
                    data_msg.yield_to_maturity = float(rec.get('YTM', '0'))
                except:
                    data_msg.yield_to_maturity = 0.0
                
                # 买卖盘口数据

                # 设置买卖盘档位数量
                try:
                    data_msg.bid_orders_num = int(rec.get('BuyOrderNo', '0'))
                except:
                    data_msg.bid_orders_num = 0
                
                try:
                    data_msg.ask_orders_num = int(rec.get('SellOrderNo', '0'))
                except:
                    data_msg.ask_orders_num = 0

                # 买盘
                buy_prices = []
                buy_volumes = []
                for i in range(1, data_msg.bid_orders_num + 1):
                    try:
                        price = float(rec.get(f'BuyPrice{i:02d}', '0'))
                        if price > 0:
                            buy_prices.append(price)
                    except:
                        pass
                    
                    try:
                        volume = int(rec.get(f'BuyVolume{i:02d}', '0'))
                        if volume > 0 or (i <= len(buy_prices)):
                            buy_volumes.append(volume)
                    except:
                        if i <= len(buy_prices):
                            buy_volumes.append(0)
                
                # 卖盘
                sell_prices = []
                sell_volumes = []
                for i in range(1, data_msg.ask_orders_num + 1):
                    try:
                        price = float(rec.get(f'SellPrice{i:02d}', '0'))
                        if price > 0:
                            sell_prices.append(price)
                    except:
                        pass
                    
                    try:
                        volume = int(rec.get(f'SellVolume{i:02d}', '0'))
                        if volume > 0 or (i <= len(sell_prices)):
                            sell_volumes.append(volume)
                    except:
                        if i <= len(sell_prices):
                            sell_volumes.append(0)
                
                # 添加到 protobuf 消息
                data_msg.bid_price.extend(buy_prices)
                data_msg.bid_volume.extend(buy_volumes)
                data_msg.ask_price.extend(sell_prices)
                data_msg.ask_volume.extend(sell_volumes)
                
                # ETF申购笔数
                try:
                    data_msg.etf_buy_count = int(rec.get('ETFBuyNo', '0'))
                except:
                    data_msg.etf_buy_count = 0
                try:
                    data_msg.etf_sell_count = int(rec.get('ETFSellNo', '0'))
                except:
                    data_msg.etf_sell_count = 0
                try:
                    data_msg.etf_buy_balance = float(rec.get('ETFBuyAmount', '0'))
                except:
                    data_msg.etf_buy_balance = 0.0
                try:
                    data_msg.etf_sell_balance = float(rec.get('ETFSellAmount', '0'))
                except:
                    data_msg.etf_sell_balance = 0.0
                try:
                    data_msg.etf_buy_volume = int(rec.get('ETFBuyVolumn', '0'))
                except:
                    data_msg.etf_buy_volume = 0
                try:
                    data_msg.etf_sell_volume = int(rec.get('ETFSellVolumn', '0'))
                except:
                    data_msg.etf_sell_volume = 0
                
                # 撤单笔数
                try:
                    data_msg.cancel_buy_num = int(rec.get('ETFBuyNo', '0'))
                except:
                    data_msg.cancel_buy_num = 0
                try:
                    data_msg.cancel_sell_num = int(rec.get('ETFSellNo', '0'))
                except:
                    data_msg.cancel_sell_num = 0
                try:
                    data_msg.cancel_buy_value = float(rec.get('ETFBuyAmount', '0'))
                except:
                    data_msg.cancel_buy_value = 0.0
                try:
                    data_msg.cancel_sell_value = float(rec.get('ETFSellAmount', '0'))
                except:
                    data_msg.cancel_sell_value = 0.0
                try:
                    data_msg.cancel_buy_volume = int(rec.get('ETFBuyVolumn', '0'))
                except:
                    data_msg.cancel_buy_volume = 0
                try:
                    data_msg.cancel_sell_volume = int(rec.get('ETFSellVolumn', '0'))
                except:
                    data_msg.cancel_sell_volume = 0

                # 设置买卖盘档位数量
                try:
                    data_msg.total_buy_num = int(rec.get('TotalBuyNo', '0'))
                except:
                    data_msg.total_buy_num = 0
                
                try:
                    data_msg.total_sell_num = int(rec.get('TotalSellNo', '0'))
                except:
                    data_msg.total_sell_num = 0

                # 委托成交最大等待时间（SH)
                try:
                    data_msg.duration_after_buy = int(rec.get('MaxBuyDuration', '0'))
                except:
                    data_msg.duration_after_buy = 0
                
                try:
                    data_msg.duration_after_sell = int(rec.get('MaxSellDuration', '0'))
                except:
                    data_msg.duration_after_sell = 0
                
                envelope.secu_depth_market_data.CopyFrom(data_msg)
            
            # 使用 CSV 中的 UNIX 字段作为 header 的 timestamp
            timestamp = rec.get('UNIX', 0)
            body = envelope.SerializeToString()
            body_length = len(body)
            header = struct.pack('=qqII', record_count, int(timestamp), msg_type, body_length)
            bin_f.write(header)
            bin_f.write(body)
            
            # 写入索引项：序列号 (int64) 与当前偏移量 (uint64)
            index_entry = struct.pack('=qQ', record_count, current_offset)
            idx_f.write(index_entry)
            
            current_offset += 24 + body_length
            record_count += 1
        
        bin_f.close()
        idx_f.close()
        
        # 写入头文件：记录总数（8字节 int64）
        with open(self.hdr_file, 'wb') as hdr_f:
            hdr_f.write(struct.pack('=q', record_count))
        
        self.logger.info(f"已写入 {record_count} 条记录到 {self.bin_file}，索引文件 {self.idx_file}，头文件 {self.hdr_file}。")
    
    def write_all(self):
        """执行 CSV 读取及二进制写入的完整流程"""
        self.logger.info("开始将 CSV 数据转换写入二进制文件...")
        self.read_csv_files()
        self.write_bin_files()
        self.logger.info("转换写入完成。")