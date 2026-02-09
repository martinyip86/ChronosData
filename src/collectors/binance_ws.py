import polars as pl
import ccxt.pro as ccxt
import asyncio
import time
import os
from datetime import datetime,timezone
from src.collectors.base import BaseController
from src.models.schema import TickData,TradeData
from src.utils.logger import logger

class BinanceController(BaseController):
    def __init__(self,symbol:str):
        super().__init__(symbol,'Binance')
        self.client = ccxt.binance({
            'enableRateLimit':True,
            'options':{'defaultType':'spot'}
        })

    async def connect(self):
        self.is_running = True
        print(f"Connected to {self.exchange_id} for {self.symbol}")
        logger.info(f"🟢 [WS-CONNECT] Connected Established | exchange: {self.exchange_id} | symbol: {self.symbol}")

    async def fetch_data(self):
        await asyncio.gather(
            self._watch_orderbook(),
            self._watch_trades()
        )

    async def _watch_orderbook(self):
        logger.info(f"📡 Start Orderbook Listener Thread: {self.symbol}")
        while self.is_running:
            try:
                orderbook = await self.client.watch_order_book(self.symbol)
                ts = orderbook.get('timestamp') or int(time.time() * 1000)
                tick = TickData(
                    symbol=self.symbol,
                    source=self.exchange_id,
                    bid_price=orderbook['bids'][0][0],
                    bid_volume=orderbook['bids'][0][1],
                    ask_price=orderbook['asks'][0][0],
                    ask_volume=orderbook['asks'][0][1],
                    bids=orderbook['bids'][:20],
                    asks=orderbook['asks'][:20],
                    lastUpdateId=orderbook['nonce'],
                    timestamp=int(ts),
                    exchange_time=int(ts)
                )
                await self.queue.put(tick)
            except Exception as e:
                print(f"Orderbook Error: {e}")
                logger.error(f"🚨 [OB-ERROR] {self.symbol} listener Exception: {e}")
                await asyncio.sleep(5)

    async def _watch_trades(self):
        logger.info(f"📡 Start Trades Listener Thread: {self.symbol}")
        while self.is_running:
            try:
                trades_list = await self.client.watch_trades(self.symbol)
                for trade_dict in trades_list:
                   trade = TradeData.from_ccxt(trade_dict,self.exchange_id)
                   await self.queue.put(trade)
            except Exception as e:
                logger.error(f"🚨 [TD-ERROR] {self.symbol} listener Exception: {e}")
                await asyncio.sleep(5)

    async def _fetch_trades(self):
        trades_list = await self.client.fetch_trades(self.symbol,since=1770186109334,limit=1000)
        for trade_dict in trades_list:
            trade = TradeData.from_ccxt(trade_dict,self.exchange_id)
            await self.queue.put(trade)

    async def storage_worker(self):
        logger.info(f"💾 Storage Worker started | Reflash range: 10s")
        buffers = {'orderbook':[],'trades':[]}
        save_interval = 10
        last_time = time.time()

        while self.is_running:
            try:
                try:
                    data = await asyncio.wait_for(self.queue.get(),timeout=1.0)
                    dtype = 'orderbook' if isinstance(data,TickData) else 'trades'
                    buffers[dtype].append(data.model_dump())
                except asyncio.TimeoutError:
                    pass

                if (time.time() - last_time) > save_interval:
                    for dtype,buffer in buffers.items():
                        if not buffer: continue
                        
                        df = pl.DataFrame(buffer)
                        file_path = self.get_save_path(exchange=self.exchange_id,symbol=self.symbol,data_type=dtype)
                        if os.path.exists(file_path):
                            exist_df = pl.read_parquet(file_path)
                            commbine_df = pl.concat([exist_df,df]).unique().sort('timestamp')
                            commbine_df.write_parquet(file_path,compression="snappy")
                        else:
                            df.write_parquet(file_path,compression='snappy')

                        start_save = time.time()
                        logger.info(f"✅ [SAVE] {self.symbol} | Type: {dtype} | Writed: {len(buffer)} | Elapsed time: {time.time()-start_save:.2f}s")
                        buffers[dtype] = []
                    last_time = time.time()
            except Exception as e:
                logger.error(f"❌ [STORAGE-CRITICAL] {self.symbol} save failed: {e}")

    def get_save_path(self,exchange:str,symbol:str,data_type:str,market_type='spot'):
        clear_symbol = symbol.replace('/','-').replace(':','-')
        now = datetime.now(timezone.utc)
        file_dir = os.path.join(
            "data/raw",
            exchange,
            market_type,
            clear_symbol,
            data_type,
            now.strftime('%Y'),
            now.strftime('%m'),
            now.strftime('%d')
        )
        os.makedirs(file_dir,exist_ok=True)
        data_name =  'ob' if data_type == 'orderbook' else 'trade'
        file_name = f"{now.strftime('%Y%m%d_%H')}_{data_name}.parquet"
        return os.path.join(file_dir,file_name)

    async def stop(self):
        await self.client.close()
        await super().stop()
                
