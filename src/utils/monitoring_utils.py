import time
from prometheus_client import REGISTRY,push_to_gateway
from src.utils.logger import setup_logger
from src.monitoring.metrics import quant_data_integrity_score,quant_last_trade_delay_ms,quant_patch_hearbeat

logger = setup_logger(name="worker-patcher")

def report_swiss_metrics(symbol:str,rows_count,is_perfect,last_ts_ms,patch_count=0):

    common_symbol = {'symbol':symbol.replace('-','_'),'exchange':'binance'}

    try:
        quant_data_integrity_score.labels(**common_symbol).set(100 if is_perfect else 0)
        current_ms = int(time.time() * 1000)
        delay_ms = current_ms - last_ts_ms
        quant_last_trade_delay_ms.labels(**common_symbol).set(max(0,delay_ms))
        quant_patch_hearbeat.labels(**common_symbol).set(time.time())
        push_to_gateway('http://pushgateway:9091',job='binance_patcher',registry=REGISTRY,timeout=3)

        logger.info(f"🇨🇭 [Swiss-Grade] Metrics for {symbol} pushed successfully.")
    except Exception as e:
        logger.error(f"🚨 [Monitoring Error] Swiss metrics push failed: {e}")