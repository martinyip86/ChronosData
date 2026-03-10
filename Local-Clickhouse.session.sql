CREATE TABLE IF NOT EXISTS market_data.lake_trades_processed (
    trade_id String,
    symbol String,
    mkt_type String,
    exchange_id String,
    trade_date Date,
    hour UInt8,
    price Float64,
    amount Float64,
    turnover Float64,
    side String,
    is_taker_buyer UInt8,
    sub_ms_seq UInt64,
    ma_price_100 Float64,
    is_high_impact UInt8,
    timestamp Int64
)
ENGINE = File(Parquet,'data/processed/*/*/*/trades/*.parquet')