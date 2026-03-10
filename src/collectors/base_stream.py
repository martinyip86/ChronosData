from abc import ABC,abstractmethod
import asyncio

class BaseStream(ABC):
    def __init__(self,exchange_id,symbol,redis_client,dtype):
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.redis = redis_client
        self.dtype = dtype
        self.is_running = False

    @abstractmethod
    async def connect(self):
        raise NotImplementedError("You must implement the connect() method to connect to exchenge")
    
    # @abstractmethod
    # async def parse_trade(self):
    #     raise NotImplementedError("You must implement the parse_trade() to process the raw data")

    async def run(self):
        self.is_running = True
        print(f"🚀 [SYSTEM] {self.exchange_id} 启动: {self.symbol}")
        while self.is_running:
            try:
                await self.connect()
            except Exception as e:
                if not self.is_running:
                    break
                print(f"🚨 [ERROR] {self.exchange_id} 连接异常: {e}")
                print(f"🔄 [RETRY] 30秒后尝试重连...")
                await asyncio.sleep(30)
        
        print(f"🏁 [EXIT] {self.exchange_id} 已停止。")

    
    def stop(self):
        self.is_running = False
        print(f"Shopping {self.exchange_id}: {self.symbol} collector......")