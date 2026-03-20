import redis.asyncio as redis

class RedisManager:
    """
    Asynchronous Redis Infrastructure Manager.
    Utilizes Connection Pooling to handle high-throughput market data 
    and inter-process communication (IPC) with minimal latency.
    """
    def __init__(self):
        # Local development/debug pool
        self._local_pool = redis.ConnectionPool(
            host='localhost',
            port=6380,
            db=0,
            decode_responses=True,
            max_connections=20
        )

        # Production-grade market data pool (Connects to the 'quant_redis' container)
        # Designed for high-frequency Rpush/Lpop operations.
        self._market_pool = redis.ConnectionPool(
            host='quant_redis',
            port=6379,
            db=0,
            decode_responses=True,
            max_connections=20
        )

    @property
    def market_db(self):
        """
        Returns an async Redis client for market data operations.
        Acts as the primary data bus for Tick and Trade streams.
        """
        return redis.Redis(connection_pool=self._market_pool)
    
    @property
    def local_db(self):
        """
        Returns an async Redis client for local caching or testing.
        """
        return redis.Redis(connection_pool=self._local_pool)

# Global singleton instance for centralized connection management
redis_manager = RedisManager()