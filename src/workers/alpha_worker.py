import polars as pl
import asyncio
import json
import time
from src.analytics.alpha_model import AlphaModel
from src.storage.ch_client import ch_manager
from src.storage.redis_client import redis_manager

async def run_alpha_generation(exchange_id='binance',mkt_type='spot',symbol='BTC/USDT'):
    model = AlphaModel(symbol)
    redis_client = redis_manager.market_db

    print(f"starting generate real-time for {symbol} signal")

    buffer = []
    data_list = await redis_client.lrange('market:ticks:all',0,200)

    for dict in data_list:
        buffer.append(json.loads(dict))

    df = pl.DataFrame(buffer)

    df = df.with_columns([
        ((pl.col('bid_prices').list.get(0) + pl.col('ask_prices').list.get(0)) / 2).alias('mid_price')
    ]).filter(pl.col('symbol')==symbol).select(['bid_prices','bid_volumes','ask_prices','ask_volumes','timestamp','symbol','mid_price'])

    result_df = model.generate_signal(df)

    print(result_df)

    latest_signal = result_df.tail(1)
    score = latest_signal['combined_alpha'][0]
    price = latest_signal['mid_price'][0]

    print(f"Time: {time.time()} | Price: {price:.2f} | Alpha Score: {score:.4f}")

    if score > 0.05:
        print(">>> 强力看涨信号，准备买入")
    elif score < -0.05:
        print("<<< 强力看跌信号，准备卖出")

    time.sleep(1) # 根据 Tick 频率调整

if __name__ == '__main__':
    asyncio.run(run_alpha_generation())