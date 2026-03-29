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
from src.monitoring.metrics import parquet_write_duration,redis_mem_gauge,sync_lag_gauge

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
        self.active_streams = {
            'orderbook':{},
            'trades':{}
        }

        self.config = {
            'orderbook':{
                'redis_key':'stream:market:ticks',
                'table':'orderbook',
                'batch_size':10000,
                'flush_interval':10.0
            },
            'trades':{
                'redis_key':'stream:market:trades',
                'table':'trades',
                'batch_size':10000,
                'flush_interval':10.0
            }
        }

    async def update_subscriptions(self,dtype):
        registry_key = f"registry:streams:{'orderbook' if dtype=='orderbook' else 'trades'}"
        while True:
            try:
                remote_keys = await self.redis.smembers(registry_key)
                for rkey in remote_keys:
                    rkey = rkey.decode() if isinstance(rkey,bytes) else rkey
                    if rkey not in self.active_streams[dtype]:
                        self.logger.info(f"🆕 [NEW-SYMBOL] Found new stream: {rkey}")
                        self.active_streams[dtype][rkey] = '$'

                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error(f"❌ [REGISTRY-ERROR] {e}")
                await asyncio.sleep(5)

    async def storage_worker(self,dtype):
        """
        Main worker loop for a specific data type (trades or orderbook).
        """
        asyncio.create_task(self.update_subscriptions(dtype))

        conf = self.config[dtype]
        registry_key = f"registry:streams:{'orderbook' if dtype=='orderbook' else 'trades'}"
        # redis_key = conf['redis_key']
        table = conf['table']
        batch_size = conf['batch_size']
        flush_interval = conf['flush_interval']
        buffer = []
        last_flush = time.time()

        self.logger.info(f"✅ [WORKER-START] Syncer active for {dtype}. Listening on: {registry_key}")

        while True:
            if not self.active_streams[dtype]:
                await asyncio.sleep(1)
                continue

            try:
                # Monitor Redis queue depth for Prometheus metrics
                # q_len = await self.redis.llen(redis_key)
                # total_pedding = 0
                # for s_keys in list(self.active_streams[dtype].keys()):
                #     q_len = await self.redis.xlen(s_keys)
                #     _,ex_k,mkt_k,sym_k,dt_k = s_keys.split(":")
                #     queue_size_gauge.labels(
                #         exchange=ex_k,
                #         mkt_type=mkt_k,
                #         symbol=sym_k,
                #         type=dt_k
                #     ).set(q_len)

                #     total_pedding += q_len

                # queue_size_gauge.labels(
                #     exchange='all',
                #     mkt_type='all',
                #     symbol='all',
                #     type=dtype
                # ).set(total_pedding)

                # Batch POP from Redis to minimize IO roundtrips
                # data_list = await self.redis.execute_command('LPOP',redis_key,batch_size)

                # if data_list:
                #     for item in data_list:
                #         clear_item = json.loads(item)
                #         # Deserialize JSON objects
                #         buffer.append(clear_item)
                # else:
                #     # Adaptive polling: sleep if queue is empty
                #     await asyncio.sleep(0.5)
                #     continue
                #     # Check for time-based flush even if no new data arrived

                response = await self.redis.xread(
                    self.active_streams[dtype],
                    count=batch_size,
                    block=500
                )

                if response:
                    for stream_key,messages in response:
                        stream_key = stream_key.decode() if isinstance(stream_key,bytes) else stream_key
                        for msg_id,content in messages:
                            buffer.append(json.loads(content['data']))
                            self.active_streams[dtype][stream_key] = msg_id

                    sync_lag_gauge.labels(data_type=dtype).set(len(buffer))

                now = time.time()
                # Trigger Flush Condition: Buffer is full OR Time limit exceeded
                if len(buffer) >= batch_size or (now - last_flush > flush_interval and buffer):
                    await self._flush(buffer,table)
                    buffer.clear()
                    last_flush = time.time()

                if not response:
                    await asyncio.sleep(0.1)

            except Exception as e:
                self.logger.error(f"❌ [INGESTION-ERROR] Failed to fetch from Redis: {e}")
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

    async def system_monitor_task(self):
        while True:
            try:
                mem_info = await self.redis.info('memory')

                redis_mem_gauge.labels(type='used_bytes').set(mem_info['used_memory'])
                redis_mem_gauge.labels(type='fragmentation').set(mem_info['mem_fragmentation_ratio'])

                if mem_info['used_memory'] > 2.5 * 1024 * 1024 * 1024:
                    self.logger.critical("🚨 [MEM-CRITICAL] Redis memory > 2.5GB! System at risk.")
                    for dtype in self.active_streams:
                        for s_k in list(self.active_streams[dtype].keys()):
                            await self.redis.xtrim(s_k,maxlen=5000,approximate=True)
                            self.active_streams[dtype][s_k] = '$'
                            self.logger.warning(f"🧹 [TRIMMED] Stream {s_k} reset to HEAD.")

                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(10)

    async def run(self):
        """Orchestrates parallel syncer workers."""
        asyncio.create_task(self.system_monitor_task())
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
                    job="market_db_syncer",
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