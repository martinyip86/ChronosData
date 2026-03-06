import asyncio
import ccxt.pro as ccxt_pro
import time
from src.collectors.base_stream import BaseStream
from src.models.schema import TickData,TradeData
from src.utils.logger import setup_logger

class BinanceStream(BaseStream):
    def __init__(self,symbol,redis_client,dtype):
        super().__init__('binance',symbol,redis_client,dtype)
        self.client = None
        self.logger = setup_logger("collector.ws.binance")

    def _create_client(self):
        config = {
            'enableRateLimit':True,
            'options':{'defaultType':'spot'}
        }
        return ccxt_pro.binance(config)

    async def connect(self):
        if self.client:
            await self.client.close()

        self.client = self._create_client()

        try:
            if self.dtype == 'orderbook':
                await self._watch_orderbook()
            else:
                await self._watch_trade()
        finally:
            await self.client.close()
            


    async def _watch_orderbook(self):
        while self.is_running:
            try:
                orderbook = await asyncio.wait_for(
                    self.client.watch_order_book(self.symbol),
                    timeout=20
                )
                
                ts = orderbook.get('timestamp',int(time.time() * 1000))
                top_20_bids = orderbook['bids'][:20]
                top_20_asks = orderbook['asks'][:20]
                tick = TickData(
                    symbol=self.symbol,
                    source=self.exchange_id,
                    bid_price=orderbook['bids'][0][0],
                    bid_volume=orderbook['bids'][0][1],
                    ask_price=orderbook['asks'][0][0],
                    ask_volume=orderbook['asks'][0][1],
                    bid_prices=[row[0] for row in top_20_bids],
                    bid_volumes=[row[1] for row in top_20_bids],
                    ask_prices=[row[0] for row in top_20_asks],
                    ask_volumes=[row[1] for row in top_20_asks],
                    lastUpdateId=orderbook['nonce'],
                    timestamp=int(ts),
                    exchange_time=int(ts)
                )
                await self.redis.rpush("market:ticks:all",tick.model_dump_json())
            except asyncio.TimeoutError:
                self.logger.warning(f"{self.exchange_id}:{self.symbol}:orderbook_ws timeout,reconnecting...")
                await asyncio.sleep(30)
                
            except Exception as e:
                self.logger.error(f"🚨 [OB-ERROR] listener Exception: {e}")
                break
            

    async def _watch_trade(self):
        while self.is_running:
            try:
                trades_list = await asyncio.wait_for(
                    self.client.watch_trades(self.symbol),
                    timeout=20
                )

                for trade_dict in trades_list:
                    trade = TradeData.from_ccxt(trade_dict,self.exchange_id)
                    #后补即时反补函数
                    await self.redis.rpush(f"market:trades:all",trade.model_dump_json())

            except asyncio.TimeoutError:
                self.logger.warning(f"{self.exchange_id}:{self.symbol}:trade_ws timeout,reconnecting...")
                await asyncio.sleep(30)

            except Exception as e:
                self.logger.error(f"🚨 [TD-ERROR] listener Exception: {e}")
                break
            