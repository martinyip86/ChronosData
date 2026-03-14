import asyncio
from src.collectors.binance_ws import BinanceController
import time
import json
import random
import requests
import os
from dotenv import load_dotenv
from datetime import datetime,timedelta,timezone
import redis.asyncio as redis
import polars as pl
import shutil
import zipfile
import io
import ccxt.pro as ccxt_pro
from src.utils.logger import setup_logger

logger = setup_logger("test_add_queue")

load_dotenv()

REDIS_CLIENT = redis.Redis(host='quant_redis',port=6379,db=0,decode_responses=True)

async def main():

    target_date = 30
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=target_date)

    print(f"🧹 开始清理 {target_date} 天前的本地归档数据 (早于 {cutoff_date.strftime('%Y-%m-%d')})")

    base_path = 'data/raw'

    if not os.path.exists(base_path):
        return False
    
    for root,dirs,files in os.walk(base_path):
        if len(dirs) == 0 and len(files) > 0:
            path_part = root.split(os.sep)
            try:
                year,month,day = int(path_part[-3]),int(path_part[-2]),int(path_part[-1])
                dir_date = datetime(year,month,day,tzinfo=timezone.utc)

                if dir_date < cutoff_date:
                    # shutil.rmtree(root)
                    print(f"🗑️ 已删除过期目录: {root}")

            except Exception as e:
                print(f"error: {e}")

    # df = pl.read_parquet("data/raw/binance/spot/BTC-USDT/trades/2026/03/10/trades_20260310_00.parquet")

    # print(df.head(1))

    # # 1. 检查数据行数是否连续（检查 nonce 跳跃）
    # df = df.with_columns(
    #     nonce_diff = pl.col("nonce").diff()
    # )

    # # 2. 计算 Imbalance 的分布（看看有没有长尾异常值）
    # print(df["imbalance"].describe())

    # # 3. 看看 Spread 是不是一直都很窄（BTC 的流动性证明）
    # print(f"Average Spread: {df['spread'].mean()}")

    # # 4. 看看有没有重复的时间戳（高频数据的大忌）
    # duplicates = df.filter(pl.col("nonce").is_duplicated()).shape[0]
    # print(f"Duplicate nonces: {duplicates}")

    # df = pl.scan_csv('temp/BTC-USDT-trades-2026-03-10.csv').collect()
    # print(df.head(1))
    

if __name__ == "__main__":
    asyncio.run(main())

{
    'code': '0', 
    'data': [
        {
            'dateAggrType': 'daily', 
            'details': [
                {
                    'ccy': '', 
                    'dateRangeEnd': '1773072000000', 
                    'dateRangeStart': '1773072000000', 
                    'groupDetails': [
                        {
                            'dateTs': '1773072000000', 
                            'filename': 'BTC-USDT-trades-2026-03-10.zip', 
                            'sizeMB': '6.13', 
                            'url': 'https://static.okx.com/cdn/okex/traderecords/trades/daily/20260310/BTC-USDT-trades-2026-03-10.zip'
                        }
                    ], 
                    'groupSizeMB': '6.13', 
                    'instFamily': '', 
                    'instId': 'BTC-USDT', 
                    'instType': 'SPOT'
                }
            ], 
            'totalSizeMB': '6.13', 
            'ts': '1773320742871'
        }
    ], 
    'msg': ''
}