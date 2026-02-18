import asyncio
from src.collectors.binance_ws import BinanceController
import json
import polars as pl

async def main():
    collector = BinanceController(symbol="BTC/USDT")

    await collector.connect()

    print("--- 开始获取 Binance 实时订单簿数据 (前3次更新) ---")

    for i in range(3):
        try:
            # orderbook = await collector.client.watch_order_book(collector.symbol)
            # print(type(orderbook['timestamp']))
            # print(f"bids: {orderbook['bids'][0]} | asks: {orderbook['asks'][0]} | timestamp: {orderbook['timestamp']} | datetime: {orderbook['datetime']} | nonce: {orderbook['nonce']} | symbol: {orderbook['symbol']}")
            # trades = await collector.client.watch_trades(collector.symbol)
            # print(trades[0].keys())
            # print(trades[0])
            dir_path = "data/raw/Binance/spot/BTC-USDT/orderbook/2026/02/04/*.parquet"
            df = pl.scan_parquet(dir_path)
            df = df.sort('nonce')
            df = df.with_columns(
                pl.col('nonce').shift(1).alias('before_nonce'),
                pl.col('nonce').alias('after_nonce')
            ).with_columns((pl.col('after_nonce') - pl.col('before_nonce')).alias('diff')).filter(pl.col('diff') > 1).select(['bid_volume','ask_volume','nonce','timestamp']).collect()

            print(df)
            
        except Exception as e:
            print(f"error: {e}")
        finally:
            print(f"正在关闭，释放资源")
            await collector.client.close()

if __name__ == "__main__":
    asyncio.run(main())