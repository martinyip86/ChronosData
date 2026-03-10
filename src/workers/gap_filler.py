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
    def __init__(self):
        self.redis = redis_manager.market_db
        self.logger = setup_logger('worker.gap_filler',log_file='logs/syncer/gap_filler.log')
        self.clients = {
            'binance':{}
        }

    async def run(self):
        await self.setup()

        self.logger.info("🛠️ [GAP-FILLER] 反补工人已就位，等待任务...")

        while True:
            try:
                result = await self.redis.blpop("queue:gap_fill_jobs",timeout=5)
                if not result:
                    continue

                _, job_data = result

                job = json.loads(job_data)
                await self.process_job(job)

                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"💥 [FILLER-CRITICAL] 运行异常: {e}")
                await asyncio.sleep(1)

    async def setup(self):
        for key in self.clients.keys():
            self.clients[key] = self._create_client(key)


    def _create_client(self,exchange_id):
        if exchange_id == 'binance':
            return ccxt_pro.binance({
                'enableRateLimit':True,
                'options':{'defaultType':'spot'},
                'apiKey': os.getenv('BINANCE_API_KEY'),
                'secret': os.getenv('BINANCE_SECRET'),
            })

    async def process_job(self,job):
        symbol:str = job['symbol']
        exchange_id = job['exchange_id']
        start_id = int(job['start_id'])
        end_id = int(job['end_id'])

        if exchange_id == 'binance':
            await self.binance_filler(exchange_id,symbol,start_id,end_id)
        

    async def binance_filler(self,exchange_id,symbol:str,start_id:int,end_id:int):
        current_from_id = start_id
        total_patched = 0

        client = self.clients[exchange_id]

        while current_from_id <= end_id:
            try:
                
                params = {
                    'symbol':symbol.replace('/','').replace('-',''),
                    'fromId':current_from_id,
                    'limit':1000
                }

                trades = await asyncio.wait_for(
                    client.publicGetHistoricalTrades(params),
                    timeout=20
                )

                for trade in trades:
                    trade_id = int(trade['id'])
                    if trade_id > end_id:
                        current_from_id = end_id + 1
                        break
                    
                    ccxt_format = {
                        'symbol': symbol,
                        'id': str(trade['id']),
                        'timestamp': int(trade['time']),
                        'side': 'sell' if trade['isBuyerMaker'] else 'buy',
                        'price': float(trade['price']),
                        'amount': float(trade['qty']),
                        'cost': float(trade['quoteQty']),
                        'is_taker_buyer': False if trade['isBuyerMaker'] else True,
                        'info': trade # 原始数据存入 info
                    }

                    trade_obj = TradeData.from_ccxt(ccxt_format, exchange_id)
                    await self.redis.rpush('market:trades:all',trade_obj.model_dump_json())

                    current_from_id = trade_id + 1
                    total_patched += 1

                self.logger.info(f"✅ [BACKFILL][{exchange_id}-{symbol}] 已补全 {total_patched} 条数据，当前进度 ID: {current_from_id}")

                if len(trades) < 1000:
                    current_from_id = end_id + 1 # 强制跳出 while
                    break
                
                # 防止触发 API 频率限制
                await asyncio.sleep(1)

            except asyncio.TimeoutError:
                await asyncio.sleep(30)
                continue    

            except Exception as e:
                self.logger.error(f"❌ [BACKFILL-CRITICAL] 反补过程中断: {e}")

if __name__ == '__main__':
    filler = GapFiller()
    asyncio.run(filler.run())
