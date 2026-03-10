import asyncio
from src.collectors.binance_ws import BinanceController
import time
import json
import random
import redis.asyncio as redis
import polars as pl
import ccxt.pro as ccxt_pro
from src.utils.logger import setup_logger

logger = setup_logger("test_add_queue")

REDIS_CLIENT = redis.Redis(host='quant_redis',port=6379,db=0,decode_responses=True)

async def main():
    # logger.info("🧪 开始往 Redis 泵入 5000 条随机成交数据...")
    # base_price = 65000.0

    # for i in range(5000):
    #     price_change = random.uniform(-10,10)
    #     current_price = round(base_price + price_change,2)
    #     amount = round(random.uniform(0.001,0.5),4)
    #     side = random.choice(['buy','sell'])

    #     trade_data = {
    #         "symbol":"BTC/USDT",
    #         "price":current_price,
    #         "amount":amount,
    #         "side":side,
    #         "timestamp": int(time.time() * 1000)
    #     }

    #     await REDIS_CLIENT.rpush('queue:trades',json.dumps(trade_data))

    #     if (i + 1) % 1000 == 0:
    #         logger.info(f"已写入 {i + 1} 条数据...")

    # logger.success("✅ 5000 条测试数据已全部进入 Redis 队列！")
    # await REDIS_CLIENT.aclose()
    # 读取你刚刚导出的 47MB 文件
    df = pl.read_parquet("data/processed/binance/spot/BTC-USDT/orderbook/20260308.parquet")

    print(df.head(1))

    # 1. 检查数据行数是否连续（检查 nonce 跳跃）
    df = df.with_columns(
        nonce_diff = pl.col("nonce").diff()
    )

    # 2. 计算 Imbalance 的分布（看看有没有长尾异常值）
    print(df["imbalance"].describe())

    # 3. 看看 Spread 是不是一直都很窄（BTC 的流动性证明）
    print(f"Average Spread: {df['spread'].mean()}")

    # 4. 看看有没有重复的时间戳（高频数据的大忌）
    duplicates = df.filter(pl.col("nonce").is_duplicated()).shape[0]
    print(f"Duplicate nonces: {duplicates}")

if __name__ == "__main__":
    asyncio.run(main())