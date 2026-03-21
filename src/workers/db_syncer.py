import asyncio
import time
from datetime import datetime
import polars as pl
import sys
import json
from src.storage.redis_client import redis_manager
from src.storage.ch_client import ch_manager
from src.utils.logger import setup_logger
from prometheus_client import push_to_gateway,REGISTRY,generate_latest
from src.monitoring.metrics import silence_gauge,parquet_write_duration,queue_size_gauge

class DBsyncer():
    """
    High-Performance Data Persistence Engine.
    Consumes market data streams from Redis and performs batch inserts into ClickHouse.
    Implements a dual-trigger flush mechanism (Batch Size & Time Interval).
    """
    def __init__(self):
        self.redis = redis_manager.market_db
        self.ch = ch_manager.market_db
        self.logger = setup_logger("worker.syncer",log_file="logs/syncer/syncer.log")

        self.config = {
            'orderbook':{
                'redis_key':'market:ticks:all',
                'table':'orderbook',
                'batch_size':10000,
                'flush_interval':10.0
            },
            'trades':{
                'redis_key':'market:trades:all',
                'table':'trades',
                'batch_size':10000,
                'flush_interval':10.0
            }
        }


    async def storage_worker(self,dtype):
        """
        Main worker loop for a specific data type (trades or orderbook).
        """
        conf = self.config[dtype]
        redis_key = conf['redis_key']
        table = conf['table']
        batch_size = conf['batch_size']
        flush_interval = conf['flush_interval']
        buffer = []
        last_flush = time.time()

        self.logger.info(f"✅ [WORKER-START] Syncer active for {dtype}. Listening on: {redis_key}")

        while True:
            try:
                # Monitor Redis queue depth for Prometheus metrics
                q_len = await self.redis.llen(redis_key)
                queue_size_gauge.labels(
                    redis_key=redis_key
                ).set(q_len)

                # Batch POP from Redis to minimize IO roundtrips
                data_list = await self.redis.execute_command('LPOP',redis_key,batch_size)

                if data_list:
                    for item in data_list:
                        clear_item = json.loads(item)
                        # Deserialize JSON objects
                        buffer.append(clear_item)
                else:
                    # Adaptive polling: sleep if queue is empty
                    await asyncio.sleep(0.5)
                    continue
                    # Check for time-based flush even if no new data arrived

                now = time.time()
                # Trigger Flush Condition: Buffer is full OR Time limit exceeded
                if len(buffer) >= batch_size or (now - last_flush > flush_interval and buffer):
                    await self._flush(buffer,table)
                    buffer.clear()
                    last_flush = time.time()

            except Exception as e:
                print(f"❌ [INGESTION-ERROR] Failed to fetch from Redis: {e}")
                await asyncio.sleep(1) # Backoff on error

    async def _flush(self,data,table_name):
        """
        Executes high-speed batch insertion into ClickHouse.
        """
        if not data:
            return

        start = time.time()

        df = pl.DataFrame(data)

        try:
            # Optimized ClickHouse insertion settings
            arrow_table = df.to_arrow()
            self.ch.insert_arrow(table=table_name,arrow_table=arrow_table)
            duration = time.time()-start
            self.logger.info(f"🚢 [FLUSH] Table: {table_name} | Rows: {len(df)} | Latency: {duration:.3f}s")
            parquet_write_duration.labels(
                table=table_name
            ).observe(duration)

            # Health warning if DB ingestion is lagging behind data stream
            if duration > self.config[table_name]['flush_interval'] * 0.8:
                self.logger.warning(f"⚠️ [PRESSURE] DB write latency is nearing limit for {table_name}!")
        except Exception as e:
            self.logger.error(f"🔥 [DB-CRITICAL] Failed to insert into {table_name}: {e}")

    async def run(self):
        """Orchestrates parallel syncer workers."""
        asyncio.create_task(self.push_metrics_periodically())
        
        # Run trades and orderbook workers concurrently
        await asyncio.gather(
            self.storage_worker('trades'),
            self.storage_worker('orderbook')
        )

    async def push_metrics_periodically(self):
        """Background task for Prometheus Pushgateway synchronization."""
        while True:
            try:
                await asyncio.to_thread(
                    push_to_gateway,
                    'http://pushgateway:9091',
                    job="market_db_syncer",  # 同步器作为另一个 job
                    registry=REGISTRY
                )
            except: pass
            await asyncio.sleep(10)

if __name__ == "__main__":
    db_syncer = DBsyncer()
    try:
        asyncio.run(db_syncer.run())
    except KeyboardInterrupt:
        pass