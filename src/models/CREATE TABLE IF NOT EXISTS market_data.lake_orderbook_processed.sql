CREATE TABLE IF NOT EXISTS market_data.lake_orderbook_processed (
    nonce Int64,
    symbol String,
    mkt_type String,
    exchange_id String,
    orderbook_date Date,
    hour UInt8,
    micro_price Float64,
    imbalance Float64,
    spread Float64,
    bid_prices Array(Float64),
    bid_volumes Array(Float64),
    ask_prices Array(Float64),
    ask_volumes Array(Float64),
    timestamp Int64
)
ENGINE = File(Parquet,'data/processed/*/*/*/orderbook/*.parquet')