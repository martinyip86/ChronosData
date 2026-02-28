import asyncio
from src.collectors.binance_ws import BinanceController
import json
import sys
print(f"当前 Python 路径: {sys.executable}")
import redis
import polars as pl

async def main():
    collector = BinanceController(symbol="BTC/USDT")

    await collector.connect()

    print("--- 开始获取 Binance 实时订单簿数据 (前3次更新) ---")

    try:
        # file_path = 'data/raw/Binance/spot/BTC-USDT/trades/2026/02/27/20260227_060237_936601_trade.parquet'
        # df = pl.read_parquet(file_path)
        # print(df.head(1))
        params = {
            'symbol':'BTCUSDT',
            'fromId':'6025330808',
            'limit':1000
        }
        trade_rest_data = await collector.client_rest.publicGetHistoricalTrades(params)
        print(trade_rest_data[0])
        
    except Exception as e:
        print(f"error: {e}")
    finally:
        print(f"正在关闭，释放资源")
        await collector.client_rest.close()
        

if __name__ == "__main__":
    asyncio.run(main())