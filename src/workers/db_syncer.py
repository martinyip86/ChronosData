import asyncio
import time
import polars as pl
from src.storage.redis_client import redis_manager
from src.storage.ch_client import ch_manager
from src.utils.logger import setup_logger

class DBsyncer():
    def __init__(self):
        self.redis = redis_manager.local_db
        self.ch = ch_manager.market_db
        self.logger = setup_logger("worker.syncer")

        self.config = {
            'orderbook':{
                'redis_key':'market:ticks:all',
                'table':'orderbook',
                'batch_size':5000,
                'flush_interval':1.0
            },
            'trades':{
                'redis_key':'market:trades:all',
                'table':'trades',
                'batch_size':1000,
                'flush_interval':2.0
            }
        }

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
                data_list = await self.redis.execute_command('LPOP',redis_key,batch_size)

                if data_list:
                    for item in data_list:
                        buffer.append(item)

                now = time.time()
                if len(buffer) >= batch_size or (now - last_flush > flush_interval and buffer):
                    await self._flush(buffer,table)

            except Exception as e:
                pass

    async def _flush(self,data,table_name):
        start = time.time()
        try:
            df = pl.DataFrame(data)
            local_ts = int(time.time() * 1000)
            
            df = df.with_columns(pl.lit(local_ts).alias('local_timestamp'))

            table_schema = self.ch.query(f"DESCRIBE TABLE {table_name}")
            valid_columns = table_schema.result_rows
            col_names = [row[0] for row in valid_columns]
            final_cols = [col for col in df.columns if col in col_names]
            df = df.select(final_cols)
            self.ch.insert_df(table=table_name,df=df)

            self.logger.info(f"🚢 [{table_name}] 写入 {len(data)} 条，耗时 {time.time()-start:.3f}s")
        except Exception as e:
            self.logger.error(f"🔥 [{table_name}] 写入失败: {e}")

    async def run(self):
        await asyncio.gather(
            self.storage_worker('trades'),
            self.storage_worker('orderbook')
        )

if __name__ == "__main__":
    db_syncer = DBsyncer()
    asyncio.run(db_syncer)