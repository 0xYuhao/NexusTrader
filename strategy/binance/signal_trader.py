import zmq
import orjson
from zmq.asyncio import Context
from decimal import Decimal
import time
from typing import Dict, List
from datetime import datetime

from nexustrader.core.log import SpdLog
from nexustrader.constants import settings
from nexustrader.config import Config, PublicConnectorConfig, PrivateConnectorConfig, BasicConfig, ZeroMQSignalConfig
from nexustrader.strategy import Strategy
from nexustrader.constants import ExchangeType, OrderSide, OrderType, KlineInterval, PositionSide
from nexustrader.exchange.binance import BinanceAccountType
from nexustrader.schema import BookL1, Kline, AccountBalance, Position, Order
from nexustrader.engine import Engine
from nexustrader.core.entity import RateLimit, DataReady
from collections import defaultdict

SpdLog.initialize(level="DEBUG", std_level="ERROR", production_mode=True)

BINANCE_API_KEY = settings.BINANCE.FUTURE.TESTNET_1.API_KEY
BINANCE_SECRET = settings.BINANCE.FUTURE.TESTNET_1.SECRET

context = Context()
socket = context.socket(zmq.SUB)
socket.connect("ipc:///tmp/zmq_data_test")
socket.setsockopt(zmq.SUBSCRIBE, b"")

class SignalTrader(Strategy):
    def __init__(self):
        super().__init__()
        self.symbols = ["BTCUSDT-PERP.BINANCE"]
        self.signal = True
        self.multiplier = 1
        self.data_ready = DataReady(symbols=self.symbols)
        self.prev_target = defaultdict(Decimal)
        self.orders = {}
        
        # 记录信号时间和价格以便跟踪是否应该平仓
        self.signal_records = {}
        self.hour_candles = defaultdict(list)
        self.hour_candle_count = defaultdict(int)
        
        # 重试配置
        self.retry_attempts = 3
        self.retry_wait_seconds = 5
        
        print("signal_trader init")
    
    def on_start(self):
        self.subscribe_bookl1(symbols=self.symbols)
        self.subscribe_kline(symbols=self.symbols, interval=KlineInterval.HOUR_1)
        
        # 从缓存加载订单信息
        self.load_orders_from_cache()
    
    def load_orders_from_cache(self):
        """从缓存中加载订单信息到self.orders字典中"""
        try:
            print("从缓存加载订单信息...")
            
            # 获取所有打开的订单
            open_orders = set()
            for symbol in self.symbols:
                # 获取该交易对的所有未完成订单
                symbol_open_orders = self.cache.get_open_orders(symbol=symbol, exchange=ExchangeType.BINANCE)
                open_orders.update(symbol_open_orders)
            
            if not open_orders:
                print("缓存中没有未完成的订单")
                return
            
            # 加载订单详情
            for uuid in open_orders:
                order_maybe = self.cache.get_order(uuid)
                if order_maybe.is_some():
                    order = order_maybe.unwrap()
                    # 只处理属于当前交易对的订单
                    if order.symbol in self.symbols:
                        self.orders[order.symbol] = uuid
                        print(f"加载订单: {order.symbol}, UUID: {uuid}, 状态: {order.status}")
            
            print(f"从缓存加载了 {len(self.orders)} 个订单")
            
        except Exception as e:
            print(f"加载订单信息时出错: {str(e)}")
            self.log.error(f"加载订单信息时出错: {str(e)}")
    
    def check_account_data(self):
        """检查账户数据是否可用，不可用时抛出异常"""
        # 检查USDT余额
        print("检查USDT余额...")
        usdt_balance = self.get_balance_safe("USDT")
        if usdt_balance <= Decimal("0"):
            print("USDT余额为0或无法获取")
            raise ValueError("USDT余额为0或无法获取")
        
        # 检查仓位信息
        for symbol in self.symbols:
            print(f"检查仓位信息: {symbol}")
            position = self.cache.get_position(symbol).value_or(None)
            if position is None:
                print(f"无法获取仓位数据: {symbol}")
                raise ValueError(f"无法获取仓位数据: {symbol}")
    
    def print_account_data(self):
        """打印当前账户数据"""
        for symbol in self.symbols:
            position = self.cache.get_position(symbol).value_or(None)
            if position is None:
                self.log.error(f"无法获取仓位数据: {symbol}")
                raise ValueError(f"无法获取仓位数据: {symbol}")
            self.log.info(f"仓位 {symbol}: {position.signed_amount}")
        
        usdt_balance = self.get_balance("USDT")
        if usdt_balance <= Decimal("0"):
            self.log.error("USDT余额为0或无法获取")
            raise ValueError("USDT余额为0或无法获取")
        self.log.info(f"USDT余额: {usdt_balance}")
    
    def get_balance_safe(self, asset: str) -> Decimal:
        """从缓存中获取特定资产的余额，如果找不到返回0而不是抛出异常"""
        try:
            result = self.get_balance(asset)
            print(f"获取到 {asset} 余额: {result}")
            return result
        except ValueError as e:
            print(f"获取 {asset} 余额错误: {str(e)}")
            return Decimal("0")
    
    def on_bookl1(self, bookl1: BookL1):
        self.data_ready.input(bookl1)
    
    def on_balance(self, balance: AccountBalance):
        self.log.info(f"余额更新: {balance.balance_total}")
    
    def get_balance(self, asset: str) -> Decimal:
        """从缓存中获取特定资产的余额"""
        # 获取账户类型对应的余额信息
        account_balance = self.cache.get_balance(BinanceAccountType.USD_M_FUTURE_TESTNET)
        print(f"asset: {asset}, account_balance: {account_balance}")
        
        # 获取特定资产的余额
        if asset in account_balance.balance_total:
            return account_balance.balance_total[asset]
        
        # 如果找不到资产余额，抛出异常
        self.log.error(f"找不到资产 {asset} 的余额")
        raise ValueError(f"找不到资产 {asset} 的余额")
    
    def on_kline(self, kline: Kline):
        symbol = kline.symbol
        print(f"Received kline for {symbol}: open={kline.open}, close={kline.close}, confirm={kline.confirm}")
        
        # 只处理已确认的1小时K线
        if kline.interval == KlineInterval.HOUR_1 and kline.confirm:
            # 记录这个小时的K线
            self.hour_candles[symbol].append(kline)
            self.hour_candle_count[symbol] += 1
            
            # 检查是否需要平仓
            if symbol in self.signal_records:
                entry_time, trend, importance = self.signal_records[symbol]
                
                # 计算自信号开始后的小时线个数
                candle_time = datetime.fromtimestamp(kline.start / 1000)
                entry_datetime = datetime.fromtimestamp(entry_time / 1000)
                
                print(f"Checking exit condition: Entry time={entry_datetime}, Current candle time={candle_time}")
                print(f"Hour candle count for {symbol}: {self.hour_candle_count[symbol]}")
                
                position = self.cache.get_position(symbol).value_or(None)
                print(f"has position: {position}")
                if not position or not position.is_opened:
                    return
                
                # 根据持仓方向和K线情况判断是否平仓
                position_is_long = position.signed_amount > 0
                candle_is_up = kline.close > kline.open
                
                exit_condition_met = False
                
                # 检查是否是第二个小时K线
                if self.hour_candle_count[symbol] >= 2:
                    # 多头在收涨时平仓，空头在收跌时平仓
                    if (position_is_long and candle_is_up) or (not position_is_long and not candle_is_up):
                        exit_condition_met = True
                        
                if exit_condition_met:
                    print(f"Exit condition met for {symbol}, closing position")
                    self.close_position(symbol)
                    del self.signal_records[symbol]
                    self.hour_candle_count[symbol] = 0
                    self.hour_candles[symbol] = []
    
    def close_position(self, symbol):
        position = self.cache.get_position(symbol).value_or(None)
        if position and position.is_opened:
            side = OrderSide.SELL if position.signed_amount > 0 else OrderSide.BUY
            self.create_order(
                symbol=symbol,
                side=side,
                type=OrderType.MARKET,
                amount=abs(position.signed_amount),
                reduce_only=True,
                account_type=BinanceAccountType.USD_M_FUTURE_TESTNET,
            )
            print(f"Closed position for {symbol}")
    
    def cal_diff(self, symbol, target_position) -> tuple[Decimal, bool]:
        """
        计算当前仓位与目标仓位的差值并确定是否为减仓操作
        """
        position = self.cache.get_position(symbol).value_or(None)
        if position is None:
            self.log.error(f"无法获取仓位数据: {symbol}")
            raise ValueError(f"无法获取仓位数据: {symbol}")
            
        current_amount = position.signed_amount
        diff = target_position - current_amount
        
        reduce_only = False

        if diff != 0:
            if abs(current_amount) > abs(target_position) and current_amount * target_position >= 0:
                reduce_only = True
            self.log.info(f"symbol: {symbol}, current {current_amount} -> target {target_position}, reduce_only: {reduce_only}")
        return diff, reduce_only
    
    def on_accepted_order(self, order: Order):
        """订单被接受时的回调"""
        print(f"订单被接受: {order.symbol}, UUID: {order.uuid}, 订单ID: {order.id}")
        # 更新订单缓存
        self.orders[order.symbol] = order.uuid
    
    def on_filled_order(self, order: Order):
        """订单完成时的回调"""
        print(f"订单完成: {order.symbol}, UUID: {order.uuid}, 订单ID: {order.id}")
        # 从订单缓存中移除
        if order.symbol in self.orders and self.orders[order.symbol] == order.uuid:
            del self.orders[order.symbol]
    
    def on_canceled_order(self, order: Order):
        """订单取消时的回调"""
        print(f"订单取消: {order.symbol}, UUID: {order.uuid}, 订单ID: {order.id}")
        # 从订单缓存中移除
        if order.symbol in self.orders and self.orders[order.symbol] == order.uuid:
            del self.orders[order.symbol]
        
    def on_custom_signal(self, signal):
        signal = orjson.loads(signal)
        print("signal_trader on_custom_signal", signal)
        
        for pos in signal:
            if not self.data_ready.ready:
                self.log.info("Data not ready, skip")
                continue
            
            # 将BTCUSDT.BBP转换为BTCUSDT-PERP.BINANCE格式
            symbol = pos["instrumentID"].replace("USDT.BBP", "USDT-PERP.BINANCE")
            
            if symbol not in self.symbols:
                self.log.info(f"symbol: {symbol} not in self.symbols, skip")
                continue
            
            # 获取信号趋势方向和重要性
            trend = pos.get("trend", 0)  # 0=中性，1=看涨，2=看跌
            importance = pos.get("importance", 0.0)
            
            # 如果是中性信号，跳过
            if trend == 0:
                self.log.info(f"Neutral signal for {symbol}, skip")
                continue
            
            # 计算资金账户中的USDT余额
            usdt_balance = self.get_balance("USDT")
            print(f"USDT Balance: {usdt_balance}")
            
            # 获取当前价格
            book = self.cache.bookl1(symbol)
            if not book:
                self.log.info(f"No price data for {symbol}, skip")
                continue
            
            current_price = (book.bid + book.ask) / 2
            
            # 根据重要性决定仓位大小，重要性满分10分
            # 使用重要性作为最大资金使用比例的因子(最大使用100%资金)
            position_ratio = (importance / 10) * 1
            
            # 计算可用资金
            # 乘以杠杆倍率，假设杠杆为5倍
            leverage = 10
            usable_usdt = usdt_balance * Decimal(str(position_ratio)) * Decimal(str(leverage))
            
            # 计算购买数量
            amount = usable_usdt / Decimal(str(current_price))
            amount = self.amount_to_precision(symbol, amount)
            
            # 如果趋势是看跌，数量为负数（做空）
            if trend == 2:  # 看跌
                amount = -amount
            
            # 记录本次信号信息
            self.signal_records[symbol] = (self.clock.timestamp_ms(), trend, importance)
            self.hour_candle_count[symbol] = 0
            self.hour_candles[symbol] = []
            
            # 创建订单
            if amount != 0:
                diff, reduce_only = self.cal_diff(symbol, amount)
                if diff != 0:
                    order_side = OrderSide.BUY if diff > 0 else OrderSide.SELL
                    order_id = self.create_order(
                        symbol=symbol,
                        side=order_side,
                        type=OrderType.MARKET,
                        amount=abs(diff),
                        account_type=BinanceAccountType.USD_M_FUTURE_TESTNET,
                        reduce_only=reduce_only,
                    )
                    print(f"Created {order_side} order for {symbol}, amount: {abs(diff)}")
                    self.orders[symbol] = order_id
                
            self.prev_target[symbol] = amount

config = Config(
    strategy_id="binance_signal_trader",
    user_id="user_signal",
    strategy=SignalTrader(),
    basic_config={
        ExchangeType.BINANCE: BasicConfig(
            api_key=BINANCE_API_KEY,
            secret=BINANCE_SECRET,
            testnet=True,
        )
    },
    public_conn_config={
        ExchangeType.BINANCE: [
            PublicConnectorConfig(
                account_type=BinanceAccountType.USD_M_FUTURE_TESTNET,
            ),
        ]
    },
    private_conn_config={
        ExchangeType.BINANCE: [
            PrivateConnectorConfig(
                account_type=BinanceAccountType.USD_M_FUTURE_TESTNET,
                rate_limit=RateLimit(
                    max_rate=20, # 20 orders per second
                    time_period=1,
                )
            )
        ]
    },
    zero_mq_signal_config=ZeroMQSignalConfig(
        socket=socket,
    ),
)

engine = Engine(config)

if __name__ == "__main__":
    try:
        engine.start()
    finally:
        engine.dispose() 