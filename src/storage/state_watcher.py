from src.storage.ch_client import ch_manager
from src.storage.redis_client import redis_manager
import asyncio

class StateWatcher:
    def __init__(self,interval=10):
        self.redis = redis_manager.market_db
        self.interval = interval

    async def update_states(self):
        print("启动 state watcher")
        while True:
            try:
                client = ch_manager.market_db
                sql = """
                    SELECT 
                        exchange_id,symbol,MAX(trade_id) AS trade_id
                    FROM market_data.trades
                    GROUP BY exchange_id,symbol
                """

                result = await asyncio.to_thread(client.query,sql)
                # self.ch_client.close()
                if result.result_rows:
                    redis_hex_key = None
                    state_map = {}
                    for row in result.result_rows:
                        if not redis_hex_key: redis_hex_key = f"cache:{row[0]}:last_trade_id"
                        state_map[row[1]] = row[2]

                    if state_map:
                        await self.redis.hset(redis_hex_key,mapping=state_map)

                    print("✅ [STATE] 状态同步成功")

            except Exception as e:
                print(f"🚨 [STATE-ERROR] 同步失败: {e}")
                continue

            await asyncio.sleep(self.interval)

    @classmethod
    async def run(cls,interval=10):
        watcher = cls(interval)
        await watcher.update_states()