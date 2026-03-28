import redis.asyncio as redis
import os
from dotenv import load_dotenv

load_dotenv()

class RedisManager:
    """
    Asynchronous Redis Infrastructure Manager.
    Utilizes Connection Pooling to handle high-throughput market data 
    and inter-process communication (IPC) with minimal latency.
    """
    def __init__(self):
        # Production-grade market data pool (Connects to the 'quant_redis' container)
        # Designed for high-frequency Rpush/Lpop operations.
        raw_host = os.getenv('REDIS_HOST', '127.0.0.1')
        host = '127.0.0.1' if raw_host == 'quant_redis' and not self._is_in_docker() else raw_host

        self._market_pool = redis.ConnectionPool(
            host=host,
            port=os.getenv('REDIS_PORT'),
            password=os.getenv('REDIS_PASSWORD'),
            db=0,
            decode_responses=True,
            max_connections=20
        )

    def _is_in_docker(self):
        return os.path.exists('/.dockerenv')

    @property
    def market_db(self):
        """
        Returns an async Redis client for market data operations.
        Acts as the primary data bus for Tick and Trade streams.
        """
        return redis.Redis(connection_pool=self._market_pool)

# Global singleton instance for centralized connection management
redis_manager = RedisManager()