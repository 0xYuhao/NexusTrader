from nexustrader.constants import settings
from nexustrader.config import Config, PublicConnectorConfig, BasicConfig, MockConnectorConfig
from nexustrader.strategy import Strategy
from nexustrader.constants import ExchangeType
from nexustrader.exchange.binance import BinanceAccountType
from nexustrader.schema import BookL1, Order
from nexustrader.engine import Engine
from nexustrader.core.log import SpdLog
from nexustrader.constants import OrderSide, OrderType, KlineInterval
from decimal import Decimal
import pandas as pd

SpdLog.initialize(level="DEBUG", std_level="ERROR", production_mode=True)


BINANCE_API_KEY = settings.BINANCE.FUTURE.TESTNET_1.API_KEY
BINANCE_SECRET = settings.BINANCE.FUTURE.TESTNET_1.SECRET



class SimpleMAStrategy(Strategy):
    def __init__(self):
        super().__init__()
        self.fast_ma_period = 5
        self.slow_ma_period = 20
        self.position_size = Decimal("0.001")
        self.in_position = False
    
    def on_start(self):
        # 获取历史数据
        end_time = self.clock.timestamp_ms()
        klines = self.request_klines(
            symbol="BTCUSDT-PERP.BINANCE",
            account_type=BinanceAccountType.USD_M_FUTURE_TESTNET,
            interval=KlineInterval.MINUTE_15,
            limit=100,
            end_time=end_time,
        )
        
        # 转换为DataFrame
        data = {
            "timestamp": [kline.start for kline in klines],
            "close": [kline.close for kline in klines],
        }
        self.df = pd.DataFrame(data)
        
        # 计算移动平均线
        self.df['fast_ma'] = self.df['close'].rolling(self.fast_ma_period).mean()
        self.df['slow_ma'] = self.df['close'].rolling(self.slow_ma_period).mean()
        
        # 订阅市场数据
        self.subscribe_bookl1(symbols=["BTCUSDT-PERP.BINANCE"])
        self.subscribe_kline(
            symbols=["BTCUSDT-PERP.BINANCE"], 
            interval=KlineInterval.MINUTE_15
        )
    
    def on_kline(self, kline):
        # 更新数据
        new_row = pd.DataFrame({
            'timestamp': [kline.start],
            'close': [kline.close]
        })
        self.df = pd.concat([self.df, new_row], ignore_index=True)
        
        # 重新计算指标
        self.df['fast_ma'] = self.df['close'].rolling(self.fast_ma_period).mean()
        self.df['slow_ma'] = self.df['close'].rolling(self.slow_ma_period).mean()
        
        # 交易逻辑
        if len(self.df) < self.slow_ma_period:
            return
            
        # 获取最新的MA值
        current_fast_ma = self.df['fast_ma'].iloc[-1]
        current_slow_ma = self.df['slow_ma'].iloc[-1]
        previous_fast_ma = self.df['fast_ma'].iloc[-2]
        previous_slow_ma = self.df['slow_ma'].iloc[-2]
        
        # 金叉信号
        if previous_fast_ma <= previous_slow_ma and current_fast_ma > current_slow_ma and not self.in_position:
            self.create_order(
                symbol="BTCUSDT-PERP.BINANCE",
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                amount=self.position_size,
            )
            self.in_position = True
            
        # 死叉信号
        elif previous_fast_ma >= previous_slow_ma and current_fast_ma < current_slow_ma and self.in_position:
            self.create_order(
                symbol="BTCUSDT-PERP.BINANCE",
                side=OrderSide.SELL,
                type=OrderType.MARKET,
                amount=self.position_size,
            )
            self.in_position = False
    
    def on_filled_order(self, order):
        print(f"订单已成交: {order.side} {order.amount} @ {order.average}")

# 配置回测环境
config = Config(
    strategy_id="ma_crossover_backtest",
    user_id="user_test",
    strategy=SimpleMAStrategy(),
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
            )
        ]
    },
    private_conn_config={
        ExchangeType.BINANCE: [
            MockConnectorConfig(
                account_type=BinanceAccountType.LINEAR_MOCK,
                initial_balance={"USDT": 10_000},
                fee_rate=0.0005,
                quote_currency="USDT",
                update_interval=20,
                leverage=1,
                overwrite_balance=True,
            )
        ]
    },
    db_path="./backtest_results.db",
)

engine = Engine(config)

if __name__ == "__main__":
    try:
        engine.start()
    finally:
        engine.dispose()
