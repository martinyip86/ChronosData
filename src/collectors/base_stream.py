from abc import ABC, abstractmethod
import asyncio
import random
from src.utils.logger import setup_logger

class BaseStream(ABC):
    def __init__(self, exchange_id, symbol, ch_client, redis_client, dtype):
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.redis = redis_client
        self.ch_client = ch_client
        self.dtype = dtype
        self.logger = setup_logger(f"collector.ws.{exchange_id}", log_file=f"logs/collector/collector_{exchange_id}.log")
        self.is_running = False
        self._stop_event = asyncio.Event()
        self.first_message_received = False

    """
    Binance-specific WebSocket implementation.
    Handles real-time trade streams and maintains sequence integrity.
    """

    @abstractmethod
    async def connect(self):
        """
        Implements the Binance WebSocket connection and subscription logic.
        Uses a resilient connection strategy with automated heartbeats.
        """
        raise NotImplementedError("You must implement the connect() method to connect to the exchange")
    
    # @abstractmethod
    # async def parse_trade(self):
    #     raise NotImplementedError("You must implement the parse_trade() to process the raw data")

    async def run(self):
        self.is_running = True
        print(f"🚀 [SYSTEM] {self.exchange_id} Started: {self.symbol}")
        while not self._stop_event.is_set():
            # Randomized jitter to prevent thundering herd effect on reconnection
            wait_time = random.uniform(1, 10)
            await asyncio.sleep(wait_time)
            try:
                await self.connect()
            except Exception as e:
                if self._stop_event.is_set(): 
                    break
                self.logger.error(f"🚨 [ERROR] {self.exchange_id} Connection exception: {e}")
                self.logger.info(f"🔄 [RETRY] Attempting reconnection in 60 seconds...")
                await asyncio.sleep(60)
        
        self.logger.info(f"🏁 [EXIT] {self.exchange_id} has stopped.")

    def stop(self):
        self._stop_event.set()
        self.logger.info(f"Stopping {self.exchange_id}: {self.symbol} collector...")