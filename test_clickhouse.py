import redis.asyncio as redis
import polars as pl
import clickhouse_connect
import json
import time
import asyncio
from src.utils.logger import setup_logger

logger = setup_logger('clickhouse.create.trades')

CH_CLIENT = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    database='market_data',
    username='default',
    password='martin1510'
)

REDIS_CLIENT = redis.Redis(host='localhost',port=6380,db=0,decode_responses=True)

async def start_persisting():
    logger.info("📡 搬运工已上线，正在监听 Redis 队列...")

    batch_data = []
    batch_size = 1000

    while True:
        raw_data = await REDIS_CLIENT.lpop('queue:trades')

        if raw_data:
            item = json.loads(raw_data)

            row = (
                item['symbol'],
                float(item['price']),
                float(item['amount']),
                item['side'],
                item['timestamp']
            )
            batch_data.append(row)

        if len(batch_data) >= batch_size or (not raw_data and len(batch_data) > 0):
            try:
                CH_CLIENT.insert(
                    'trades',
                    batch_data,
                    column_names=['symbol','price','amount','side','timestamp']
                )
                logger.info(f"✅ 已存入 {len(batch_data)} 条数据至 ClickHouse")
                batch_data = []
            except Exception as e:
                logger.error(f"❌ 写入 ClickHouse 失败: {e}")
                time.sleep(5)

        if not raw_data:
            await asyncio.sleep(0.5)

if __name__ == '__main__':
    asyncio.run(start_persisting())
    