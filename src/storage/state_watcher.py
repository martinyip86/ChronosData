from src.storage.ch_client import ch_manager
from src.storage.redis_client import redis_manager
import asyncio

class StateWatcher:
    """
    System State Synchronizer.
    Periodically synchronizes the latest 'trade_id' from ClickHouse (source of truth) 
    to Redis (hot cache) to maintain sequence continuity across system restarts.
    """
    def __init__(self,interval=10):
        self.redis = redis_manager.market_db
        self.interval = interval

    async def update_states(self):
        """
        Background worker loop for state synchronization.
        Uses distributed polling to keep the hot cache aligned with persistent storage.
        """
        print("🚀 [SYSTEM] Starting State Watcher...")
        while True:
            try:
                # Access ClickHouse as the historical source of truth
                client = ch_manager.market_db

                # Query the maximum sequence ID per instrument to recover the latest state
                sql = """
                    SELECT 
                        exchange_id,symbol,MAX(trade_id) AS trade_id
                    FROM market_data.trades
                    GROUP BY exchange_id,symbol
                """

                # Use to_thread to prevent the blocking ClickHouse driver from stalling the event loop
                result = await asyncio.to_thread(client.query,sql)
                
                if result.result_rows:
                    # Logic to map and cache states in Redis using HASH sets
                    redis_hex_key = None
                    state_map = {}
                    for row in result.result_rows:
                        # Dynamic key generation based on exchange_id
                        if not redis_hex_key: redis_hex_key = f"cache:{row[0]}:last_trade_id"
                        state_map[row[1]] = row[2]

                    if state_map:
                        # Atomic update to Redis cache
                        await self.redis.hset(redis_hex_key,mapping=state_map)
                        print(f"✅ [STATE] Successfully synchronized {len(state_map)} instruments.")

            except Exception as e:
                # Fault tolerance: log failure and continue to the next cycle
                print(f"🚨 [STATE-ERROR] Synchronization failure: {e}")
                await asyncio.sleep(5)  # Short wait before retry on error
                continue

            # Throttle the synchronization to reduce DB overhead
            await asyncio.sleep(self.interval)

    @classmethod
    async def run(cls,interval=10):
        """
        Class method to initialize and execute the watcher.
        """
        watcher = cls(interval)
        await watcher.update_states()