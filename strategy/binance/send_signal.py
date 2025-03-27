import zmq
import orjson
import time
from decimal import Decimal

# 创建ZMQ上下文
context = zmq.Context()

# 创建发布者socket
socket = context.socket(zmq.PUB)
# IPC地址格式：
# ipc:// 是协议前缀，表示使用IPC（进程间通信）协议
# /tmp/zmq_data_test 是文件路径，表示在系统的临时目录下创建一个IPC文件
# 这个文件会作为ZMQ通信的媒介
# 绑定到相同的IPC地址
socket.bind("ipc:///tmp/zmq_data_test")

# 示例信号数据
signal_data = [
    {
        "instrumentID": "BTCUSDT.BBP",
        "position": 1.0  # 目标仓位
    },
    {
        "instrumentID": "ETHUSDT.BBP",
        "position": 2.0
    }
]

def send_signal():
    while True:
        try:
            # 将数据转换为JSON字符串
            message = orjson.dumps(signal_data)
            
            # 发送消息
            socket.send(message)
            print(f"发送信号: {signal_data}")
            
            # 等待1秒
            time.sleep(1)
            
        except KeyboardInterrupt:
            print("停止发送信号")
            break
        except Exception as e:
            print(f"发送错误: {e}")
            break

if __name__ == "__main__":
    print("开始发送信号...")
    send_signal()
    
    # 清理资源
    socket.close()
    context.term() 