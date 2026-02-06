import polars as pl
import asyncio
import datetime
import ccxt.pro as ccxt
import os
import json
import time
from src.models.schema import TradeData

CSV_PATH = "temp/BTCUSDT-trades-2026-02-04.csv"
PARQUET_PATH = "data/raw/Binance/spot/BTC-USDT/trades/2026/02/04/20260204_04_trade.parquet"

def patch_gap(symbol,start_id,end_id):
    df = pl.read_csv(CSV_PATH,has_header=False,new_columns=["trade_id","price","amount","cost","timestamp","is_maker","is_best"])
    df = df.filter(
        (pl.col('trade_id') >= start_id) & (pl.col('trade_id') <= end_id)
    ).with_columns(
        pl.lit(symbol).alias('symbol'),
        pl.lit('Binance').alias('exchange_id'),
        pl.col('trade_id').cast(pl.Utf8).alias('trade_id'),
        (pl.col("timestamp") // 1000).alias("timestamp"),
        pl.when(pl.col('is_maker')==False).then(pl.lit('buy')).otherwise(pl.lit('sell')).alias('side'),
        pl.col('is_maker').not_().alias('is_taker_buyer'),
        pl.lit(json.dumps({'info':'restored_from_official_csv'})).alias('raw_info'),
        pl.lit(int(time.time() * 1000)).alias('local_timestamp')
    )
    exist_df = pl.read_parquet(PARQUET_PATH)
    df = df.select(exist_df.columns)
    df = df.cast(exist_df.schema)
    combine_df = pl.concat([exist_df,df]).unique(subset='trade_id').sort('trade_id')
    combine_df.write_parquet(PARQUET_PATH,compression="snappy")
    print(f"✅ 成功缝合 {len(df)} 条数据！")

if __name__ == "__main__":
    start_id = 5884397029 + 1
    end_id = 5884399332 - 1
    patch_gap('BTC/USDT',start_id,end_id)