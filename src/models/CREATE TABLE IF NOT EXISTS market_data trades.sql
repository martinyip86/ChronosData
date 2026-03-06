CREATE TABLE IF NOT EXISTS market_data.trades (
    trade_id String,
    symbol LowCardinality(String),
    exchange_id LowCardinality(String),
    price Float64,
    amount Float64,
    side LowCardinality(String),
    is_taker_buyer UInt8,
    timestamp Int64,
    local_timestamp Int64,
    created_at DateTime64(3,'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(local_timestamp)
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(local_timestamp))
ORDER BY (symbol,timestamp,exchange_id,trade_id)