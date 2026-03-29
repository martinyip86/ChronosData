from prometheus_client import Gauge,Counter,Histogram,REGISTRY

ws_reconnect_total = Counter(
    "ws_reconnect_total",
    "Websocket reconnect count",
    ["exchange","symbol"]
)

ws_error_total = Counter(
    "ws_error_total",
    "Websocket error count",
    ["exchange","symbol"]
)

silence_gauge = Gauge(
    "silence_gauge",
    "Silence span",
    ["exchange","symbol","type"]
)

sync_lag_gauge = Gauge(
    "sync_backlog_count", 
    "Unprocessed messages in Redis", 
    ["data_type"]
)

parquet_write_duration = Histogram(
    "parquet_write_duration_seconds",
    "Parquet duration",
    ["table"]
)

quant_data_integrity_score = Gauge(
    "quant_data_integrity_score",
    "Data integrity score (100 max)",
    ["exchange","symbol"]
)

quant_last_trade_delay_ms = Gauge(
    "quant_last_trade_delay_ms",
    "Milliseconds since last trade timestamp",
    ["exchange","symbol"]
)

quant_patch_hearbeat = Gauge(
    "quant_patch_hearbeat",
    "Timestamp of last monitor run",
    ["exchange","symbol"]
)

redis_mem_gauge = Gauge(
    "redis_memory_gauge",
    "Redis memory monitor",
    ["type"]
)