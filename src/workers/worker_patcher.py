import polars as pl
import os
import glob
import time
import json
import requests
import zipfile
import io
from datetime import datetime,timedelta,timezone
from src.utils.logger import logger

CSV_PATH = None

def main(target_date=None):
    date_str = target_date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    dt_obj = datetime.strptime(date_str,"%Y-%m-%d")
    data_path = f"data/raw/Binance/spot/BTC-USDT/trades/{dt_obj.strftime('%Y/%m/%d')}/*.parquet"
    files = sorted(glob.glob(data_path))

    logger.info(f"🔍Start sacnning local data | Date: {date_str} | path partern: {data_path}")

    if not files:
        logger.warning(f"⚠️Local parquet data not found,check if the collector is running normally,please.")
    else:
        lf = pl.scan_parquet(files)
        report = lf.select([
            pl.len().alias('Total raw'),
            pl.col('timestamp').min().alias('Start time'),
            pl.col('timestamp').max().alias('Last time'),
            (pl.col('price').max() - pl.col('price').min()).alias('Price fluktuation range'),
            pl.col('amount').sum().alias('Total trading volume')
        ]).collect()
        logger.info(f"📊 Data overview: total row={report['Total raw'][0]} | Time span: {report['Start time'][0]} ~ {report['Last time'][0]}")

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
            max_diff = gaps['diff'].max()
            logger.error(f"🚨 Severe data gaps detected! Total gaps: {len(gaps)} | Max ID jump{max_diff}")
            if download_and_unzip(date_str):
                official_df = pl.read_csv(CSV_PATH,has_header=False,new_columns=["trade_id","price","amount","cost","timestamp","is_maker","is_best"])
                for row in gaps.iter_rows(named=True):
                    parquet_path = os.path.join(
                        "data/raw/Binance/spot/BTC-USDT/trades",
                        f"{row['dt_start'].strftime('%Y/%m/%d/%Y%m%d_%H')}_trade.parquet"
                    )
                    patch_gap('BTC/USDT',row['before_gap_id'],row['after_gap_id'],parquet_path,official_df)

                if os.path.exists(CSV_PATH):
                    os.remove(CSV_PATH)
                    print(f"易清理{CSV_PATH}文件")
            else:
                print("下载失败")        

        else:
            logger.info("✅ Perfect! Local data matches trade_id sequence,no remedy required.")

def patch_gap(symbol,start_id,end_id,parquet_path,official_df):
    if not os.path.exists(parquet_path):
        print(f"⚠️ 跳过：目标 Parquet 不存在 -> {parquet_path}")
        return
    
    df = official_df.filter(
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

def download_and_unzip(date_str):
    global CSV_PATH
    url = f"https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-{date_str}.zip"
    file_path = f"temp/BTCUSDT-trades-{date_str}.csv"
    if os.path.exists(file_path):
        print(f"file is exist {file_path}")
        CSV_PATH = file_path
        return True
    
    logger.info(f"🌐 Rquesting a patch from binance")
    start_time = time.time()
    try:
        r = requests.get(url)
        if r.status_code == 200:
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall("temp/")
            print("解压完成！")
            logger.info(f"📦 Download and zip confirmed. | Elapsed time: {time.time() - start_time:.2f}s | file path: {CSV_PATH}")
            CSV_PATH = file_path
            return True
        else:
            print(f"下载失败，状态码: {r.status_code}")
            return False
    except Exception as e:
        print(f"下载失败,error: {e}")
        return False

if __name__ == "__main__":
    main()
