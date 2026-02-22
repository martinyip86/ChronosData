from src.processors.compactor import DataCompactor as DC
import polars as pl
import asyncio
from datetime import datetime

def main():
    file_dir = "data/raw/Binance/spot/BTC-USDT/trades"
    df = DC(file_dir)
    date_obj = datetime.strptime('2026-02-04','%Y-%m-%d')
    df = df.compact_day(date_obj,'trade')

if __name__ == '__main__':
    main()