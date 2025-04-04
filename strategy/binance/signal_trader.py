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

SpdLog.initialize(level="INFO", std_level="INFO", production_mode=True)

BINANCE_API_KEY = settings.BINANCE.FUTURE.TESTNET_1.API_KEY
BINANCE_SECRET = settings.BINANCE.FUTURE.TESTNET_1.SECRET
# 杠杆倍率
LEVERAGE = 10

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
                order = order_maybe.value_or(None)
                if order is not None:
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
        self.log.debug(f"Received kline for {symbol}: open={kline.open}, close={kline.close}, confirm={kline.confirm}")

        # 只处理已确认的1小时K线
        if kline.interval == KlineInterval.HOUR_1 and kline.confirm:
            # 获取持仓信息
            position = self.cache.get_position(symbol).value_or(None)
            print(f"position: {position}")
            self.log.info(f"has position: {position}")

            if not position or not position.is_opened:
                print(f"No open position for {symbol}, skipping exit check")
                self.log.info(f"No open position for {symbol}, skipping exit check")
                return
            
            # 获取当前K线的时间
            candle_time = datetime.fromtimestamp(kline.start / 1000)
            # 获取入场时间
            entry_time = self.get_position_entry_time(symbol)
            if entry_time == 0:
                self.log.info(f"无法获取入场时间: {symbol}, 跳过出场检查")
                return
            
            entry_datetime = datetime.fromtimestamp(entry_time / 1000)
            self.log.info(f"Checking exit condition: Entry time={entry_datetime}, Current candle time={candle_time}")
            
            # 判断当前K线是否是持仓后的第二个小时K线或更晚
            # 计算小时差异
            hour_diff = (candle_time - entry_datetime).total_seconds() / 3600
            self.log.info(f"Hours since entry: {hour_diff}")
            
            # 如果小时差值大于等于1，说明至少是第二个小时K线
            is_after_first_hour = hour_diff >= 1
            
            # 根据持仓方向和K线情况判断是否平仓
            position_is_long = position.signed_amount > 0
            candle_is_up = kline.close > kline.open
            
            exit_condition_met = False
            
            # 从第二个小时K线开始评估平仓条件
            if is_after_first_hour:
                print(f"Evaluating exit condition after first hour for {symbol}")
                # 多头在收涨时平仓，空头在收跌时平仓
                if (position_is_long and candle_is_up) or (not position_is_long and not candle_is_up):
                    exit_condition_met = True
                    
            if exit_condition_met:
                print(f"Exit condition met for {symbol}, closing position")
                self.close_position(symbol)
    
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
    
    def get_position_entry_time(self, symbol: str) -> int:
        """
        获取仓位的入场时间
        
        通过查找该交易对的所有订单，找到创建当前持仓的订单，并返回其创建时间
        如果找不到相关订单，返回0
        
        :param symbol: 交易对名称
        :return: 入场时间的毫秒时间戳
        """
        # 获取当前仓位
        position = self.cache.get_position(symbol).value_or(None)
        if not position or not position.is_opened:
            self.log.debug(f"没有持仓数据: {symbol}")
            return 0
            
        # 获取该交易对的所有订单UUID
        order_uuids = self.cache.get_symbol_orders(symbol, in_mem=False)
        self.log.debug(f"获取到 {len(order_uuids)} 个订单UUID: {symbol}")
        if not order_uuids:
            self.log.debug(f"没有找到订单记录: {symbol}")
            print(f"没有找到订单记录: {symbol}")
            return 0
            
        # 排序顺序: 持仓方向 > 订单类型(非平仓) > 时间戳(最早)
        position_side = PositionSide.LONG if position.signed_amount > 0 else PositionSide.SHORT
        self.log.debug(f"当前仓位方向: {position_side}")
        entry_orders = []
        
        # 遍历所有订单，寻找开仓订单
        for uuid in order_uuids:
            order_maybe = self.cache.get_order(uuid)
            # 使用value_or方法获取订单，如果不存在则继续
            order = order_maybe.value_or(None)
            self.log.debug(f"get_position_entry_time order: {order}")
            if order is None:
                continue
                
            # 检查订单是否已成交
            if not order.is_filled:
                continue
                
            # 检查是否为开仓订单
            is_open_position = False
            # 检查订单的持仓方向是否与当前持仓方向一致
            if order.position_side == position_side:
                # 订单的持仓方向与当前持仓一致
                if not order.reduce_only:
                    # 非平仓订单
                    is_open_position = True
            # 单向模式(One-way Mode)下的情况：
            # 在Binance的单向模式中，订单的position_side可能显示为FLAT或者为空，因为单向模式下不需要指定持仓方向
            # 系统只根据订单的side(BUY/SELL)来决定是开多还是开空
            elif order.position_side is None or order.position_side == PositionSide.FLAT:
                # 单向模式(one-way mode)的订单，根据买卖方向判断
                if (position_side == PositionSide.LONG and order.side == OrderSide.BUY) or \
                   (position_side == PositionSide.SHORT and order.side == OrderSide.SELL):
                    if not order.reduce_only:
                        is_open_position = True
            
            if is_open_position:
                self.log.debug(f"找到开仓订单: {order.id}, 时间: {datetime.fromtimestamp(order.timestamp / 1000)}")
                entry_orders.append(order)
        
        # 按照时间戳排序，取最晚开仓订单
        if entry_orders:
            entry_orders.sort(key=lambda x: x.timestamp, reverse=True)
            latest_order = entry_orders[0]
            entry_time = latest_order.timestamp
            self.log.info(f"最晚开仓订单 {latest_order.id} 的时间: {datetime.fromtimestamp(entry_time / 1000)}")
            return entry_time
        
        self.log.info(f"没有找到开仓订单: {symbol}")
        return 0
    
    def on_accepted_order(self, order: Order):
        """订单被接受时的回调"""
        print(f"订单被接受: {order.symbol}, UUID: {order.uuid}, 订单ID: {order.id}")
        # 更新订单缓存
        self.orders[order.symbol] = order.uuid
    
    def on_filled_order(self, order: Order):
        """订单完成时的回调"""
        print(f"订单完成: {order.symbol}, UUID: {order.uuid}, 订单ID: {order.id}")
        self.log.info(f"订单完成: {order.symbol}, UUID: {order.uuid}, 订单ID: {order.id} timestamp: {order.timestamp}")
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
            usable_usdt = usdt_balance * Decimal(str(position_ratio)) * Decimal(str(LEVERAGE))
            
            # 计算购买数量
            amount = usable_usdt / Decimal(str(current_price))
            amount = self.amount_to_precision(symbol, amount)
            
            # 如果趋势是看跌，数量为负数（做空）
            if trend == 2:  # 看跌
                amount = -amount
            
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