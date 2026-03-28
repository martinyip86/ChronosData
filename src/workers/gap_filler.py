from src.storage.redis_client import redis_manager
from src.utils.logger import setup_logger
from src.models.schema import TradeData
import ccxt.pro as ccxt_pro
import json
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

class GapFiller:
    """
    Asynchronous Self-Healing Engine.
    Monitors the 'gap_fill_jobs' queue and fetches missing trade sequences 
    via REST API to ensure data continuity.
    """
    def __init__(self):
        self.redis = redis_manager.market_db
        self.logger = setup_logger('worker.gap_filler',log_file='logs/syncer/gap_filler.log')
        self.clients = {
            'binance':{},
            'okx':{}
        }

    async def run(self):
        """Main service loop for processing gap recovery tasks."""
        await self.setup()

        self.logger.info("🛠️ [GAP-FILLER] Worker deployed. Listening for sequence gaps...")

        while True:
            try:
                # Blocking POP from Redis: efficient and low-latency task consumption
                result = await self.redis.blpop("queue:gap_fill_jobs",timeout=5)
                if not result:
                    continue

                _, job_data = result
                job = json.loads(job_data)

                # Execute recovery logic
                await self.process_job(job)

                # Rate-limiting backoff to respect Exchange API quotas
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"💥 [FILLER-CRITICAL] Execution panic: {e}")
                await asyncio.sleep(1)

    async def setup(self):
        """Initializes REST clients for high-speed recovery fetching."""
        for key in self.clients.keys():
            self.clients[key] = self._create_client(key)


    def _create_client(self,exchange_id):
        """Exchange client factory with built-in rate limiting."""
        if exchange_id == 'binance':
            return ccxt_pro.binance({
                'enableRateLimit':True,
                'options':{'defaultType':'spot'},
                'apiKey': os.getenv('BINANCE_API_KEY'),
                'secret': os.getenv('BINANCE_SECRET'),
            })
        elif exchange_id == 'okx':
            return ccxt_pro.okx({
                'enableRateLimit':True,
                'options':{'defaultType':'spot'},
                'aiohttp_trust_env': False
            })

    async def process_job(self,job):
        await asyncio.sleep(2)
        """Dispatches job to specific exchange backfiller."""
        job_id = job['job_id']
        symbol:str = job['symbol']
        exchange_id = job['exchange_id']
        start_id = int(job['start_id'])
        end_id = int(job['end_id'])

        self.logger.info(f"🔍 [GAP-DETECTED] Recovering {exchange_id}-{symbol}: ID {start_id} -> {end_id}")
        try:
            await self.binance_filler(exchange_id,symbol,start_id,end_id)
        except Exception as e:
            self.logger.error(f"❌ [JOB-FAILED] Error during backfill: {e}")
        finally:
            if job_id:
                lock_key = "lock:gap_jobs_active"
                await self.redis.srem(lock_key, job_id)
                self.logger.info(f"🔓 [LOCK-RELEASED] Job fingerprint {job_id} cleared.")

        

    async def binance_filler(self,exchange_id,symbol:str,start_id:int,end_id:int):
        """
        Historical trade recovery logic. 
        Iterates through the missing sequence and pushes data back to the primary data bus.
        """
        current_from_id = start_id
        total_patched = 0

        symbol_key = symbol.replace('/','-')
        stream_key = f"md:{exchange_id}:spot:{symbol_key}:trades"
        registry_key = f"registry:streams:trades"

        client = self.clients[exchange_id]

        while current_from_id <= end_id:
            try:
                if exchange_id == 'binance':
                    # Binance specific historical trade endpoint (fromId based)
                    params = {
                        'symbol':symbol.replace('/','').replace('-',''),
                        'fromId':current_from_id,
                        'limit':1000
                    }

                    trades = await asyncio.wait_for(
                        client.publicGetHistoricalTrades(params),
                        timeout=20
                    )
                elif exchange_id == 'okx':
                    # OKX typically uses 'before'/ID-based pagination
                    # Implementation detail: OKX requires careful ID sorting
                    raw_trades = await client.fetch_trades(symbol, params={'before': str(current_from_id - 1)})
                    trades = sorted(raw_trades, key=lambda x: int(x['id']))

                if not trades:
                    break

                await self.redis.sadd(registry_key,stream_key)

                async with self.redis.pipeline(transaction=False) as pipe:
                    for trade in trades:
                        trade_id = int(trade['id'])

                        # Ensure we don't overshoot the gap end_id
                        if trade_id > end_id:
                            current_from_id = end_id + 1
                            break
                        
                        # Normalize raw response to internal Schema-compliant format
                        if exchange_id == 'binance':
                            # isBuyerMaker: True means Sell side for Taker
                            ccxt_format = {
                                'symbol': symbol,
                                'exchange_id': exchange_id,
                                'mkt_type': 'spot',
                                'id': str(trade['id']),
                                'timestamp': int(trade['time']),
                                'side': 'sell' if trade['isBuyerMaker'] else 'buy',
                                'price': float(trade['price']),
                                'amount': float(trade['qty']),
                                'cost': float(trade['quoteQty']),
                                'is_taker_buyer': False if trade['isBuyerMaker'] else True
                            }
                        elif exchange_id == 'okx':
                            ccxt_format = {
                                'symbol': symbol,
                                'exchange_id': exchange_id,
                                'mkt_type': 'spot',
                                'id': str(trade['id']),
                                'timestamp': int(trade['timestamp']),
                                'side': trade['side'],
                                'price': float(trade['price']),
                                'amount': float(trade['amount']),
                                'cost': float(trade['cost']),
                                'is_taker_buyer': False if trade['side'] == 'sell' else True
                            }

                        # Re-inject missing data into the main processing pipeline
                        trade_obj = TradeData.from_ccxt(ccxt_format, exchange_id)
                        # await self.redis.rpush('market:trades:all',trade_obj.model_dump_json())
                        await pipe.xadd(
                            stream_key,
                            {'data':trade_obj.model_dump_json()},
                            maxlen=10000,
                            approximate=True
                        )
                        current_from_id = trade_id + 1
                        total_patched += 1

                self.logger.info(f"✅ [BACKFILL-SYNC] {exchange_id}-{symbol} | Progress: {total_patched} trades recovered.")

                # Sequence reached its logical end
                if len(trades) < 50:
                    break
                
                await asyncio.sleep(1) # Prevent API ban

            except asyncio.TimeoutError:
                self.logger.warning("⏳ [FILLER-TIMEOUT] Exchange unresponsive. Cooling down...")
                await asyncio.sleep(30)
                continue    

            except Exception as e:
                self.logger.error(f"❌ [BACKFILL-CRITICAL] Process interrupted: {e}")
                break

if __name__ == '__main__':
    filler = GapFiller()
    asyncio.run(filler.run())
