import asyncio
from src.collectors.binance_ws import BinanceController
import json

async def main():
    collector = BinanceController(symbol="BTC/USDT")

    await collector.connect()

    print("--- 开始获取 Binance 实时订单簿数据 (前3次更新) ---")

    for i in range(3):
        try:
            # orderbook = await collector.client.watch_order_book(collector.symbol)
            # print(type(orderbook['timestamp']))
            # print(f"bids: {orderbook['bids'][0]} | asks: {orderbook['asks'][0]} | timestamp: {orderbook['timestamp']} | datetime: {orderbook['datetime']} | nonce: {orderbook['nonce']} | symbol: {orderbook['symbol']}")
            trades = await collector.client.watch_trades(collector.symbol)
            print(trades[0].keys())
            print(trades[0])
        except Exception as e:
            print(f"error: {e}")
        finally:
            print(f"正在关闭，释放资源")
            await collector.client.close()

if __name__ == "__main__":
    asyncio.run(main())