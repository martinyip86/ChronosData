import time
from prometheus_client import Gauge,CollectorRegistry,push_to_gateway
from src.utils.logger import setup_logger

logger = setup_logger(name="worker-patcher")

def report_swiss_metrics(symbol:str,rows_count,is_perfect,last_ts_ms,patch_count=0):
    #注册独立列表
    registry = CollectorRegistry()

    common_symbol = {'symbol':symbol.replace('-','_')}

    g_score = Gauge('quant_data_integrity_score','Data integrity score (100 max)',labelnames=['symbol'],registry=registry)

    g_delay = Gauge('quant_last_trade_delay_ms','Milliseconds since last trade timestamp',labelnames=['symbol'],registry=registry)

    g_heatbeat = Gauge('quant_patch_hearbeat','Timestamp of last monitor run',labelnames=['symbol'],registry=registry)

    try:
        g_score.labels(**common_symbol).set(100 if is_perfect else 0)
        current_ms = int(time.time() * 1000)
        delay_ms = current_ms - last_ts_ms
        g_delay.labels(**common_symbol).set(max(0,delay_ms))
        g_heatbeat.labels(**common_symbol).set(time.time())
        push_to_gateway('http://pushgateway:9091',job='binance_patcher',registry=registry,timeout=3)

        logger.info(f"🇨🇭 [Swiss-Grade] Metrics for {symbol} pushed successfully.")
    except Exception as e:
        logger.error(f"🚨 [Monitoring Error] Swiss metrics push failed: {e}")