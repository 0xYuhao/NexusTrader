from nexustrader.config import BasicConfig, Config, MockConnectorConfig, PublicConnectorConfig
from nexustrader.strategy import Strategy
from nexustrader.schema import BookL1, Order
from nexustrader.constants import OrderSide, OrderType, KlineInterval
from nexustrader.exchange.binance import BinanceAccountType
from nexustrader.engine import Engine
import pandas as pd
import numpy as np
from nexustrader.config import Config, PublicConnectorConfig, BasicConfig, MockConnectorConfig
from nexustrader.constants import ExchangeType
from nexustrader.exchange.binance import BinanceAccountType
from datetime import datetime
import os
import time

class HistoricalBacktestStrategy(Strategy):
    def __init__(self):
        super().__init__()
        self.symbol = "BTCUSDT-PERP.BINANCE"
        self.fast_ma_period = 5
        self.slow_ma_period = 20
        self.position_size = 0.001
        self.in_position = False
        self.trades = []
        self.initial_balance = 10000
        self.current_balance = self.initial_balance
        
    def on_start(self):
        # 读取本地历史数据
        # 获取当前目录 strategy/binance/data
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'btcusdt_1h.csv')
        self.historical_data = pd.read_csv(data_path)
        self.historical_data['timestamp'] = pd.to_datetime(self.historical_data['timestamp'])
        
        # 计算技术指标
        self.historical_data['fast_ma'] = self.historical_data['close'].rolling(self.fast_ma_period).mean()
        self.historical_data['slow_ma'] = self.historical_data['close'].rolling(self.slow_ma_period).mean()
        
        # 设置回测时间范围
        self.start_time = self.historical_data['timestamp'].min()
        self.end_time = self.historical_data['timestamp'].max()
        
        # 按时间顺序遍历历史数据进行回测
        for index, row in self.historical_data.iterrows():
            current_time = row['timestamp']
            current_price = row['close']
            current_fast_ma = row['fast_ma']
            current_slow_ma = row['slow_ma']
            
            # 交易逻辑
            if current_fast_ma > current_slow_ma and not self.in_position:
                # 开多仓
                self.create_order(
                    symbol=self.symbol,
                    side=OrderSide.BUY,
                    type=OrderType.MARKET,
                    amount=self.position_size
                )
                self.in_position = True
                self.entry_price = current_price
                self.trades.append({
                    'type': 'BUY',
                    'price': current_price,
                    'timestamp': current_time,
                    'balance': self.current_balance
                })
                
            elif current_fast_ma < current_slow_ma and self.in_position:
                # 平多仓
                self.create_order(
                    symbol=self.symbol,
                    side=OrderSide.SELL,
                    type=OrderType.MARKET,
                    amount=self.position_size
                )
                self.in_position = False
                # 计算收益
                profit = (current_price - self.entry_price) * self.position_size
                self.current_balance += profit
                self.trades.append({
                    'type': 'SELL',
                    'price': current_price,
                    'timestamp': current_time,
                    'profit': profit,
                    'balance': self.current_balance
                })
            
            # 模拟回测速度
            time.sleep(0.001)
    
    def on_stop(self):
        # 计算回测结果
        if self.trades:
            trades_df = pd.DataFrame(self.trades)
            total_trades = len(trades_df[trades_df['type'] == 'SELL'])
            total_profit = trades_df['profit'].sum()
            profit_rate = (self.current_balance - self.initial_balance) / self.initial_balance * 100
            
            print(f"\n=== 回测结果 ===")
            print(f"回测时间范围: {self.start_time} 到 {self.end_time}")
            print(f"总交易次数: {total_trades}")
            print(f"总收益: {total_profit:.2f} USDT")
            print(f"收益率: {profit_rate:.2f}%")
            print(f"最终余额: {self.current_balance:.2f} USDT")
            
            # 保存回测结果
            results_path = os.path.join(os.path.dirname(__file__), 'results', 'backtest_results.csv')
            trades_df.to_csv(results_path, index=False)
            print(f"\n回测结果已保存至: {results_path}")

config = Config(
    strategy_id="historical_backtest",
    user_id="user_test",
    strategy=HistoricalBacktestStrategy(),
    basic_config={
        ExchangeType.BINANCE: BasicConfig(
            api_key="your_api_key",
            secret="your_secret",
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
    db_path="./historical_backtest_results.db",
)



engine = Engine(config)

if __name__ == "__main__":
    try:
        engine.start()
    finally:
        engine.dispose()