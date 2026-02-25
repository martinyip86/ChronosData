from src.processors.compactor import DataCompactor as DC
import polars as pl
import asyncio
import os
import glob
from datetime import datetime

def main():
    # file_dir = "data/raw/Binance/spot/BTC-USDT/trades"
    # df = DC(file_dir)
    # date_obj = datetime.strptime('2026-02-22','%Y-%m-%d')
    # df = df.compact_day(date_obj,'trade')
    date_obj = datetime.strptime('2026-02-23','%Y-%m-%d')
    file_path = "data/raw/Binance/spot/BTC-USDT/trades"
    data_dir = os.path.join(file_path,date_obj.strftime("%Y/%m/%d"))
    files_path = os.path.join(
        data_dir,
        "*_split.parquet"
    )
    files = sorted(glob.glob(files_path))

    unique_set = ['trade_id','timestamp']

    df = pl.scan_parquet(files).unique(unique_set).sort('timestamp').collect(engine='streaming')
    mem_mb = df.estimated_size() / (1024**2)
    print(f"该文件在内存中占用: {mem_mb:.2f} MB")

if __name__ == '__main__':
    main()