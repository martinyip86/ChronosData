CREATE TABLE IF NOT EXISTS market_data.lake_orderbook_raw (
    nonce Int64,
    symbol String,
    exchange_id String,
    bid_price Float64,
    bid_volume Float64,
    ask_price Float64,
    ask_volume Float64,
    bid_prices Array(Float64),
    bid_volumes Array(Float64),
    ask_prices Array(Float64),
    ask_volumes Array(Float64),
    timestamp Int64
)
ENGINE = File(Parquet,'data/raw/*/*/*/orderbook/*/*/*/*.parquet')