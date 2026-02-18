import polars as pl
from src.utils.logger import logger

def validator_trades(df:pl.DataFrame):
    if not (df['price'] > 0).all():
        invaild_row = df.filter(pl.col('price') <= 0)
        raise ValueError(f"Abnormal price data detected: {invaild_row}")
    
    price_change = (df['price'].max() - df['price'].min()) / df['price'].min()
    if price_change > 0.1:
        logger.warning(f"⚠️ Significant fluctuations detected during data backfill: {price_change},please manual verification required.")

    return True