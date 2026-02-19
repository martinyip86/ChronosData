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
    ["exchange","symbol"]
)

queue_size_gauge = Gauge(
    "collector_queue_size",
    "Collector Queue Size",
    ["exchange","symbol"]
)

parquet_write_duration = Histogram(
    "parquet_write_duration_seconds",
    "Parquet duration",
    ["exchange","symbol","type"]
)