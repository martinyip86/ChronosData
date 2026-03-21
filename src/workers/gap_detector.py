import asyncio
import json
import hashlib
from datetime import datetime
import polars as pl
from src.storage.ch_client import ch_manager
from src.storage.redis_client import redis_manager
from src.utils.logger import setup_logger

class GapDetector:
    def __init__(self,interval=60):
        self.redis = redis_manager.market_db
        self.ch_client = ch_manager.market_db
        self.interval = interval
        self.lock_key = "lock:gap_jobs_active"
        self.logger = setup_logger("worker.gap_detector",log_file="logs/syncer/gap_detector.log")

    async def scan_and_dispatch(self):
        self.logger.info("🔍 [GAP-DETECTOR] Guard deployed. Scanning for data fractures...")
        while True:
            try:
                target_date_str = datetime.now().strftime('%Y-%m-%d')
                for exchange_id in ['binance']:
                    for symbol in ['BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT']:
                        sql = f"""
                            SELECT
                                trade_id
                            FROM trades
                            WHERE timestamp >= toUnixTimestamp64Milli(toDateTime64('{target_date_str} 00:00:00',3))
                            AND timestamp < toUnixTimestamp64Milli(toDateTime64('{target_date_str} 00:00:00',3) + INTERVAL 1 DAYS)
                            AND exchange_id='{exchange_id}'
                            AND symbol='{symbol}'
                            ORDER BY trade_id ASC
                        """

                        result_arrow = await asyncio.to_thread(self.ch_client.query_arrow,sql)
                        df = pl.from_arrow(result_arrow)
                        if not df.is_empty():
                            df = (
                                df.with_columns([
                                    pl.col("trade_id").shift(1).alias("prev_id")
                                ])
                                .filter((pl.col("trade_id") - pl.col("prev_id") > 1))
                                .select([
                                    (pl.col("prev_id") + 1).alias("gap_start"),
                                    (pl.col("trade_id") - 1).alias("gap_end")
                                ])
                            )
                            for row in df.iter_rows(named=True):
                                exchange_id = exchange_id
                                symbol = symbol
                                start_id = int(row['gap_start'])
                                end_id = int(row['gap_end'])
                                job_id = hashlib.md5(f"{exchange_id}:{symbol}:{start_id}:{end_id}".encode()).hexdigest()
                                is_new = await self.redis.sadd(self.lock_key,job_id)

                                if is_new:
                                    await self.redis.expire(self.lock_key, 3600)
                                    gap_job = {
                                        'symbol':symbol,
                                        'exchange_id':exchange_id,
                                        'start_id':start_id,
                                        'end_id':end_id,
                                        'job_id':job_id
                                    }
                                    await self.redis.rpush("queue:gap_fill_jobs",json.dumps(gap_job))
                                    self.logger.info(f"🕳️ [GAP-FOUND] {exchange_id}-{symbol}: {start_id} -> {end_id}. Job dispatched.")
                                else:
                                    pass
                        else:
                            self.logger.info("✨ [CLEAN] No gaps detected in the last 2 hours.")
            except Exception as e:
                self.logger.error(f"🚨 [DETECTOR-ERROR] Scan failed: {e}")
                await asyncio.sleep(10)
                continue

            await asyncio.sleep(self.interval)

    @classmethod
    async def run(cls,interval=60):
        detector = cls(interval)
        await detector.scan_and_dispatch()

if __name__ == '__main__':
    asyncio.run(GapDetector.run())