CREATE TABLE IF NOT EXISTS market_data.trades_pro (
    trade_id Int64 CODEC(Delta,LZ4),
    trade_id_raw String,
    symbol LowCardinality(String),
    exchange_id LowCardinality(String),
    mkt_type LowCardinality(String),
    price Float64 CODEC(Gorilla,LZ4),
    amount Float64 CODEC(Gorilla,LZ4),
    side LowCardinality(String),
    is_taker_buyer UInt8,
    timestamp Int64 CODEC(Delta,LZ4),
    local_timestamp Int64 CODEC(Delta,LZ4),
    created_at DateTime64(3,'UTC') DEFAULT now64(),
    receive_latency Alias(local_timestamp - timestamp)
)
ENGINE = ReplacingMergeTree(local_timestamp)
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(local_timestamp))
ORDER BY (exchange_id,symbol,mkt_type,trade_id)
SETTINGS index_granularity=8192,index_granularity_bytes=10485760