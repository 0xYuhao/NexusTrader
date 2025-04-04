import zmq
import orjson
import time
from decimal import Decimal
import argparse

# 创建ZMQ上下文
context = zmq.Context()

# 创建发布者socket
socket = context.socket(zmq.PUB)
# 绑定到相同的IPC地址
socket.bind("ipc:///tmp/zmq_data_test")

def send_signal(trend=1, importance=7.0):
    """
    发送交易信号
    trend: 0=中性，1=看涨，2=看跌
    importance: 重要性评分（0-10分）
    """
    # 构建信号数据
    signal_data = [
        {
            "instrumentID": "BTCUSDT.BBP",
            "wait": 5,
            "source": "white_house_news",
            "importance": importance,
            "trend": trend
        }
    ]
    
    try:
        # 将数据转换为JSON字符串
        message = orjson.dumps(signal_data)
        
        # 发送消息
        socket.send(message)
        print(f"发送信号: {signal_data}")
        
        # 等待1秒
        time.sleep(1)
        
    except Exception as e:
        print(f"发送错误: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='发送交易信号')
    parser.add_argument('--trend', type=int, choices=[0, 1, 2], default=1, 
                        help='趋势方向：0=中性，1=看涨，2=看跌')
    parser.add_argument('--importance', type=float, default=7.0, 
                        help='重要性评分（0-10分）')
    args = parser.parse_args()
    
    print(f"开始发送信号... 趋势={args.trend}, 重要性={args.importance}")
    send_signal(args.trend, args.importance)
    
    # 清理资源
    socket.close()
    context.term() 