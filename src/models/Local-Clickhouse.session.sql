CREATE TABLE IF NOT EXISTS market_data.lake_trades_raw (
    trade_id String,
    symbol String,
    exchange_id String,
    price Float64,
    amount Float64,
    side String,
    is_taker_buyer UInt8,
    timestamp Int64
)
ENGINE = File(Parquet,'data/raw/*/*/*/trades/*/*/*/*.parquet')