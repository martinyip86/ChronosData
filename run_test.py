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

REDIS_CLIENT = redis.Redis(host='localhost',port=6380,db=0,decode_responses=True)

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
    length = await REDIS_CLIENT.llen("market:trade:all")
    print(f"当前队列长度: {length}")

if __name__ == "__main__":
    asyncio.run(main())