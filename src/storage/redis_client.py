import redis.asyncio as redis

class RedisManager:
    def __init__(self):
        self._local_pool = redis.ConnectionPool(
            host='localhost',
            port=6380,
            db=0,
            decode_responses=True,
            max_connections=20
        )

        self._market_pool = redis.ConnectionPool(
            host='quant_redis',
            port=6379,
            db=0,
            decode_responses=True,
            max_connections=20
        )

    @property
    def market_db(self):
        return redis.Redis(connection_pool=self._market_pool)
    
    @property
    def local_db(self):
        return redis.Redis(connection_pool=self._local_pool)
    
redis_manager = RedisManager()