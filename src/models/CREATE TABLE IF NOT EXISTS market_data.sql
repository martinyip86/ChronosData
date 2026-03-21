CREATE TABLE IF NOT EXISTS market_data.orderbook (
    nonce Int64 CODEC(Delta,LZ4),
    symbol LowCardinality(String),
    exchange_id LowCardinality(String),
    mkt_type LowCardinality(String),
    bid_volume Float64 CODEC(Gorilla,LZ4),
    bid_price Float64 CODEC(Gorilla,LZ4),
    ask_volume Float64 CODEC(Gorilla,LZ4),
    ask_price Float64 CODEC(Gorilla,LZ4),
    bid_prices Array(Float64) CODEC(ZSTD(1)),
    bid_volumes Array(Float64) CODEC(ZSTD(1)),
    ask_prices Array(Float64) CODEC(ZSTD(1)),
    ask_volumes Array(Float64) CODEC(ZSTD(1)),
    timestamp Int64 CODEC(DoubleDelta,LZ4),
    local_timestamp Int64 CODEC(DoubleDelta,LZ4),
    created_at DateTime64(3,'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(local_timestamp)
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(local_timestamp))
ORDER BY (exchange_id,symbol,mkt_type,timestamp,nonce)
TTL fromUnixTimestamp64Milli(local_timestamp) + INTERVAL 7 DAY
SETTINGS index_granularity=8192,index_granularity_bytes=10485760