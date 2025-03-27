import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime

# 连接到SQLite数据库
conn = sqlite3.connect('strategy/backtest/backtest_results.db')

# 提取订单数据
orders_df = pd.read_sql_query("SELECT * FROM ma_crossover_backtest_user_test_orders", conn)
orders_df['timestamp'] = pd.to_datetime(orders_df['timestamp'], unit='ms')

# 提取PnL数据
pnl_df = pd.read_sql_query("SELECT * FROM ma_crossover_backtest_user_test_pnl", conn)
pnl_df['timestamp'] = pd.to_datetime(pnl_df['timestamp'], unit='ms')

# 创建输出目录
if not os.path.exists('backtest_results'):
    os.makedirs('backtest_results')

# 分析订单
print(f"总交易次数: {len(orders_df)}")
buy_orders = orders_df[orders_df['side'] == 'BUY']
sell_orders = orders_df[orders_df['side'] == 'SELL']
print(f"买入订单数: {len(buy_orders)}")
print(f"卖出订单数: {len(sell_orders)}")

# 计算盈亏情况
initial_balance = pnl_df['pnl'].iloc[0]
final_balance = pnl_df['pnl'].iloc[-1]
profit = final_balance - initial_balance
profit_percent = (profit / initial_balance) * 100

print(f"\n初始资金: {initial_balance:.2f}")
print(f"最终资金: {final_balance:.2f}")
print(f"总盈亏: {profit:.2f} ({profit_percent:.2f}%)")

# 绘制PnL曲线
plt.figure(figsize=(12, 6))
plt.plot(pnl_df['timestamp'], pnl_df['pnl'], label='PnL')
plt.title('backtest pnl curve')
plt.xlabel('time')
plt.ylabel('balance')
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig('backtest_results/pnl_curve.png')

# 绘制订单执行价格
plt.figure(figsize=(12, 6))
buy_scatter = plt.scatter(buy_orders['timestamp'], buy_orders['price'], color='green', marker='^', label='buy')
sell_scatter = plt.scatter(sell_orders['timestamp'], sell_orders['price'], color='red', marker='v', label='sell')
plt.title('order prices')
plt.xlabel('time')
plt.ylabel('price')
plt.grid(True)
plt.legend(handles=[buy_scatter, sell_scatter])
plt.tight_layout()
plt.savefig('backtest_results/order_prices.png')

# 计算交易统计
# 计算每次交易的盈亏
if len(orders_df) >= 2:
    trades = []
    for i in range(0, len(orders_df), 2):
        if i+1 < len(orders_df):
            buy_order = orders_df.iloc[i] if orders_df.iloc[i]['side'] == 'BUY' else orders_df.iloc[i+1]
            sell_order = orders_df.iloc[i+1] if orders_df.iloc[i]['side'] == 'BUY' and orders_df.iloc[i+1]['side'] == 'SELL' else orders_df.iloc[i]
            
            if buy_order['side'] == 'BUY' and sell_order['side'] == 'SELL':
                profit = (sell_order['price'] - buy_order['price']) * float(buy_order['amount'])
                trades.append({
                    'buy_time': buy_order['timestamp'],
                    'sell_time': sell_order['timestamp'],
                    'buy_price': buy_order['price'],
                    'sell_price': sell_order['price'],
                    'amount': float(buy_order['amount']),
                    'profit': profit
                })
    
    trades_df = pd.DataFrame(trades)
    if not trades_df.empty:
        print("\n交易统计:")
        print(f"盈利交易次数: {len(trades_df[trades_df['profit'] > 0])}")
        print(f"亏损交易次数: {len(trades_df[trades_df['profit'] < 0])}")
        print(f"平均每笔盈亏: {trades_df['profit'].mean():.2f}")
        print(f"最大单笔盈利: {trades_df['profit'].max():.2f}")
        print(f"最大单笔亏损: {trades_df['profit'].min():.2f}")
        
        # 保存交易明细
        trades_df.to_csv('backtest_results/trades_detail.csv', index=False)

# 关闭数据库连接
conn.close()

print("\n分析完成! 结果已保存到 'backtest_results' 文件夹") 