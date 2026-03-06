CREATE TABLE IF NOT EXISTS market_data.orderbook (
    nonce Int64,
    symbol LowCardinality(String),
    exchange_id LowCardinality(String),
    bid_volume Float64,
    bid_price Float64,
    ask_volume Float64,
    ask_price Float64,
    bid_prices Array(Float64),
    bid_volumes Array(Float64),
    ask_prices Array(Float64),
    ask_volumes Array(Float64),
    timestamp Int64,
    local_timestamp Int64,
    created_at DateTime64(3) DEFAULT now64()
)
ENGINE = ReplacingMergeTree(local_timestamp)
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(local_timestamp))
ORDER BY (symbol,timestamp,exchange_id,nonce)