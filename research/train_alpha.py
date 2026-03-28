from research.factor_analysis import AlphaResearch
from src.utils.weight_manager import WeightManager
from src.storage.ch_client import ch_manager
from datetime import datetime,timezone
import polars as pl
import glob
import sys

def load_historical_data(exchange_id:str,mkt_type:str,symbol:str,days:int=10):
    symbol = symbol.replace('/','-')
    file_paths = f"data/processed/{exchange_id}/{mkt_type}/{symbol}/orderbook/*.parquet"
    files = sorted(glob.glob(file_paths))[-days:]

    if not files:
        print("files aren't exists")
        return None

    df = pl.scan_parquet(files).with_columns([
        ((pl.col('bid_prices').list.get(0) + pl.col('ask_prices').list.get(0)) / 2).alias('mid_price')
    ]).select(['timestamp','symbol','bid_prices','bid_volumes','ask_prices','ask_volumes','mid_price'])
    return df

date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

symbol = 'BTC/USDT'

ch_client = ch_manager.market_db

sql = f"""
    SELECT
        timestamp,
        symbol,
        bid_prices,
        bid_volumes,
        ask_prices,
        ask_volumes,
        (bid_prices[1] + ask_prices[1]) / 2 as mid_price
    FROM market_data.orderbook
    WHERE exchange_id='binance' 
    AND symbol='BTC/USDT'
    AND toDate(fromUnixTimestamp64Milli(timestamp)) = '{date_str}'
    ORDER BY timestamp ASC
    LIMIT 100000
"""
arrow_table = ch_client.query_arrow(sql)
df_totay = pl.from_arrow(arrow_table).lazy()

lazy_history = load_historical_data('binance','spot',symbol)

if lazy_history is not None:
    df_final = pl.concat([lazy_history,df_totay])
else:
    df_final = df_totay

df = df_final.with_columns([
    pl.col('symbol').replace('-','/')
]).sort('timestamp').collect()

research = AlphaResearch(df)
research.compute_features().label_data().train_combined_signal()

symbol = symbol.replace('/','_').lower()
filename = f"{symbol}_weights.json"

research.weights['best_lag'] = research.best_lag
old_weight = WeightManager.load_weight(filename)

if old_weight is not None:
    alpha = 0.2
    research.weights['w_vamp'] = old_weight['w_vamp'] * (1 - alpha) + research.weights['w_vamp'] * alpha
    research.weights['w_ofi'] = old_weight['w_ofi'] * (1 - alpha) + research.weights['w_ofi'] * alpha

WeightManager.save_weight(research.weights,filename)

print(f"训练完成，模型已存至 configs/weights/{filename}")
