from abc import ABC,abstractmethod
import asyncio
import time
# from typing import Any

class BaseController(ABC):
    def __init__(self,symbol:str,exchange_id:str):
        self.symbol = symbol
        self.exchange_id = exchange_id
        self.is_running = False
        # self.client:Any = None
        self.queue = self.queue = asyncio.Queue(maxsize=50000)#内部缓冲群
        self._reset_lock = asyncio.Lock()

    @abstractmethod
    async def connect(self):
        pass

    @abstractmethod
    async def fetch_data(self):
        pass

    async def stop(self):
        self.is_running = False
        print(f"Stopping {self.exchange_id} collector...")