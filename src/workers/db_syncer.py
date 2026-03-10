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

        self.schemas = {}

        for config in self.config.values():
            table_schema = self.ch.query(f"DESCRIBE TABLE {config['table']}")
            valid_columns = table_schema.result_rows
            self.schemas[config['table']] = [row[0] for row in valid_columns if row[0] != "created_at"]


    async def storage_worker(self,dtype):
        conf = self.config[dtype]
        redis_key = conf['redis_key']
        table = conf['table']
        batch_size = conf['batch_size']
        flush_interval = conf['flush_interval']
        buffer = []
        last_flush = time.time()

        self.logger.info(f"✅ {dtype} 同步协程已启动，监听: {redis_key}")

        while True:
            try:
                q_len = await self.redis.llen(redis_key)
                queue_size_gauge.labels(
                    redis_key=redis_key
                ).set(q_len)
                data_list = await self.redis.execute_command('LPOP',redis_key,batch_size)

                if data_list:
                    for item in data_list:
                        clear_item = json.loads(item)
                        buffer.append(clear_item)
                else:
                    await asyncio.sleep(0.5)
                    continue

                now = time.time()
                if len(buffer) >= batch_size or (now - last_flush > flush_interval and buffer):
                    await self._flush(buffer,table)
                    buffer.clear()
                    last_flush = time.time()

            except Exception as e:
                print(f"获取错误：{e}")
                pass

    async def _flush(self,data,table_name):
        if not data:
            return

        start = time.time()

        final_cols = self.schemas[table_name]
        rows = [None] * len(data)
        for i, row_dict in enumerate(data):
            # 使用列表推导式配合 get，比显式循环快 20%
            rows[i] = [row_dict.get(col) for col in final_cols]
        try:
            self.ch.insert(
                table=table_name,
                data=rows,
                column_names=final_cols,
                settings={
                    'insert_distributed_sync':0,
                    'max_insert_block_size':100000
                }
            )
            duration = time.time()-start
            self.logger.info(f"🚢 [{table_name}] 写入 {len(data)} 条，耗时 {duration:.3f}s")
            parquet_write_duration.labels(
                table=table_name
            ).observe(duration)

            if duration > self.config[table_name]['flush_interval'] * 0.8:
                self.logger.warning(f"⚠️ [{table_name}] 写入压力接近极限！")
        except Exception as e:
            self.logger.error(f"🔥 [{table_name}] 写入失败: {e}")

    async def run(self):
        asyncio.create_task(self.push_metrics_periodically())
        
        await asyncio.gather(
            self.storage_worker('trades'),
            self.storage_worker('orderbook')
        )

    async def push_metrics_periodically(self):
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
    asyncio.run(db_syncer.run())