import polars as pl
import os
import glob
import time
import json
import datetime

CSV_PATH = "temp/BTCUSDT-trades-2026-02-05.csv"

def main():
    data_path = "data/raw/Binance/spot/BTC-USDT/trades/2026/02/05/*.parquet"
    files = sorted(glob.glob(data_path))

    if not files:
        print("未找到数据")
    else:
        lf = pl.scan_parquet(files)
        report = lf.select([
            pl.len().alias('Total raw'),
            pl.col('timestamp').min().alias('起始时间'),
            pl.col('timestamp').max().alias('最后时间'),
            (pl.col('price').max() - pl.col('price').min()).alias('价格波动范围'),
            pl.col('amount').sum().alias('总成交量')
        ]).collect()
        print("采集数据总体报告")
        print(report)

        gaps = lf.with_columns(pl.col('trade_id').cast(pl.Int64)).sort('trade_id').select([
            pl.col('trade_id').shift(1).alias('before_gap_id'),
            pl.col('trade_id').alias('after_gap_id'),
            pl.col('timestamp').shift(1).alias('gap_start_ts'),
            pl.col('timestamp').alias('gap_end_ts')
        ]).with_columns((pl.col('after_gap_id') - pl.col('before_gap_id')).alias('diff')).filter(pl.col('diff') > 1).with_columns([
            pl.from_epoch('gap_start_ts',time_unit='ms').alias('dt_start'),
            pl.from_epoch('gap_end_ts',time_unit='ms').alias('dt_end')
        ]).collect()

        if len(gaps) > 0:
            print(f"断档总数: {len(gaps)},最大断档市场{gaps['diff'].max() / 1000}")
            print(gaps)
            for row in gaps.iter_rows(named=True):
                parquet_path = os.path.join(
                    "data/raw/Binance/spot/BTC-USDT/trades",
                    f"{row['dt_start'].strftime('%Y/%m/%d/%Y%m%d_%H')}_trade.parquet"
                )
                patch_gap('BTC/USDT',row['before_gap_id'],row['after_gap_id'],parquet_path)

        else:
            print("\n完美")

def patch_gap(symbol,start_id,end_id,parquet_path):
    if not os.path.exists(parquet_path):
        print(f"⚠️ 跳过：目标 Parquet 不存在 -> {parquet_path}")
        return
    
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
    exist_df = pl.read_parquet(parquet_path)
    df = df.select(exist_df.columns)
    df = df.cast(exist_df.schema)
    combine_df = pl.concat([exist_df,df]).unique(subset='trade_id').sort('trade_id')
    combine_df.write_parquet(parquet_path,compression="snappy")
    print(f"✅ 成功缝合 {len(df)} 条数据！")

if __name__ == "__main__":
    main()
