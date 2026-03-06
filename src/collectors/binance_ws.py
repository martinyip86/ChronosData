import polars as pl
import ccxt.pro as ccxt
import asyncio
import time
import os
import uuid
import redis.asyncio as redis
import json
import random
from datetime import datetime,timezone
from dotenv import load_dotenv
from prometheus_client import push_to_gateway,REGISTRY,generate_latest
from src.collectors.base import BaseController
from src.models.schema import TickData,TradeData
from src.utils.logger import setup_logger
from src.monitoring.metrics import ws_reconnect_total,ws_error_total,silence_gauge,parquet_write_duration,queue_size_gauge

load_dotenv()

api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_SECRET')

class BinanceController(BaseController):
    def __init__(self,symbol:str):
        super().__init__(symbol,'Binance')
        self.client_ob = self._create_client(authenticated=False)
        self.client_trade = self._create_client(authenticated=False)
        self.client_rest = self._create_client(authenticated=True)
        self.Lock_ob = asyncio.Lock()
        self.Lock_trade = asyncio.Lock()
        self.last_reconnect_ts_ob = 0
        self.last_reconnect_ts_trade = 0
        self.is_reconnecting_ob = False
        self.is_reconnecting_trade = False
        self.last_msg_time_ob = time.time()
        self.last_msg_time_trade = time.time()
        self.logger = setup_logger(name="collector.ws.binance",log_file="logs/collector/collector.log")
        self.last_id_key = f"last_trade_id:{symbol}"
        self.dedup_prefix = f"seen:{symbol.replace('/', '')}"

        self.r = redis.Redis(host='quant_redis',port=6379,decode_responses=True)

    def _create_client(self,authenticated=False):
        config = {
            'enableRateLimit':True,
            'options':{'defaultType':'spot'}
        }
        if authenticated:
            config.update({
                'apiKey': api_key,
                'secret': api_secret,
            })
        return ccxt.binance(config)

    async def connect(self):
        wait_time = random.uniform(1, 5)
        await asyncio.sleep(wait_time)

        self.is_running = True
            
        print(f"Connected to {self.exchange_id} for {self.symbol}")
        self.logger.info(f"🟢 [WS-CONNECT] Connected Established | exchange: {self.exchange_id} | symbol: {self.symbol}")

    async def fetch_data(self):
        await asyncio.gather(
            self._watch_orderbook(),
            self._watch_trades(),
            self.watchdog(client_type='ob'),
            self.watchdog(client_type='trade'),
            self.push_metrics_periodically(),
            return_exceptions=True
        )

    async def _watch_orderbook(self):
        self.logger.info(f"📡 Start Orderbook Listener Thread: {self.symbol}")
        while self.is_running:
            if self.is_reconnecting_ob:
                await asyncio.sleep(1)
                continue
            try:
                try:
                    orderbook = await asyncio.wait_for(
                        self.client_ob.watch_order_book(self.symbol),
                        timeout=20
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("WS timeout, reconnecting...")
                    await self._reset_client(client_type='ob')
                    continue

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
                self.last_msg_time_ob = time.time()
            except Exception as e:
                print(f"Orderbook Error: {e}")
                self.logger.error(f"🚨 [OB-ERROR] {self.symbol} listener Exception: {e}")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
                await asyncio.sleep(10)

    async def _watch_trades(self):
        self.logger.info(f"📡 Start Trades Listener Thread: {self.symbol}")
        while self.is_running:
            if self.is_reconnecting_trade:
                await asyncio.sleep(1)
                continue
            try:
                try:
                    trades_list = await asyncio.wait_for(
                        self.client_trade.watch_trades(self.symbol),
                        timeout=20
                    )
                except asyncio.TimeoutError:
                    self.logger.warning("WS timeout, reconnecting...")
                    await self._reset_client(client_type='trade')
                    continue

                for trade_dict in trades_list:
                    trade = TradeData.from_ccxt(trade_dict,self.exchange_id)

                    curr_id = int(trade.trade_id)

                    last_id_str = await self.r.get(self.last_id_key)
                    if last_id_str:
                        last_id = int(last_id_str)

                        if curr_id > last_id + 1:
                            asyncio.create_task(self._backfill_trade(last_id + 1, curr_id - 1))

                    if await self.r.setnx(f"{self.dedup_prefix}:{curr_id}",1):
                        await self.r.expire(f"{self.dedup_prefix}:{curr_id}",3600)
                        await self.r.set(self.last_id_key,curr_id)

                        await self.queue.put(trade)
                    self.last_msg_time_trade = time.time()
            except Exception as e:
                self.logger.error(f"🚨 [TD-ERROR] {self.symbol} listener Exception: {e}")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
                await asyncio.sleep(10)

    async def storage_worker(self):
        self.logger.info(f"💾 Storage Worker started | Reflash range: 10s")
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
                        
                        start_save = time.time()
                        df = pl.DataFrame(buffer)
                        file_path = self.get_save_path(exchange=self.exchange_id,symbol=self.symbol,data_type=dtype)
                        temp_path = f"{file_path}.{uuid.uuid4().hex}.tmp"
                        try:
                            df.write_parquet(temp_path,compression='snappy')
                            os.replace(temp_path,file_path)
                            # if os.path.exists(file_path):
                            #     await asyncio.to_thread(self._sync_save,file_path,df,dtype)
                            # else:
                            #     df.write_parquet(file_path,compression='snappy')

                            self.logger.info(f"✅ [SAVE] {self.symbol} | Type: {dtype} | Writed: {len(buffer)} | Elapsed time: {time.time()-start_save:.2f}s")
                        except Exception as e:
                            self.logger.error(f"❌ [SAVE-ERROR] Atomic write failed: {e}")
                            if os.path.exists(temp_path):
                                os.remove(temp_path)
                        parquet_write_duration.labels(
                            exchange=self.exchange_id,
                            symbol=self.symbol,
                            type=dtype
                        ).observe(time.time() - start_save)
                        buffers[dtype] = []
                    last_time = time.time()
            except Exception as e:
                self.logger.error(f"❌ [STORAGE-CRITICAL] {self.symbol} save failed: {e}")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()

    def _sync_save(self,file_path,df,dtype):
        exist_df = pl.read_parquet(file_path)
        subset = ['timestamp','trade_id'] if dtype == 'trades' else ['timestamp','nonce']
        commbine_df = pl.concat([exist_df,df]).unique(subset=subset).sort('timestamp')
        commbine_df.write_parquet(file_path,compression="snappy")          

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
        file_name = f"{now.strftime('%Y%m%d_%H%M%S_%f')}_{data_name}.parquet"
        return os.path.join(file_dir,file_name)
    
    async def _reset_client(self,client_type='ob'):
        lock = self.Lock_ob if client_type == 'ob' else self.Lock_trade
        async with lock:
            last_reconnect_time = self.last_reconnect_ts_ob if client_type == 'ob' else self.last_reconnect_ts_trade
            time_now = time.time()
            if (time_now - last_reconnect_time) < 10:
                self.logger.info(f"🛑 [SKIP] {client_type} was reset {time_now - last_reconnect_time:.1f}s ago. Ignoring this request.")
                return

            self.logger.warning(f"🔄 [RESET] Rebuilding {client_type} client...")

            if client_type == 'ob':
                self.is_reconnecting_ob = True
            else:
                self.is_reconnecting_trade = True
            
            old_client = self.client_ob if client_type == 'ob' else self.client_trade
            try:
                await old_client.close()
            except Exception:
                pass

            await asyncio.sleep(1)
            
            try:
                new_client = self._create_client(authenticated=False)
                if client_type == 'ob':
                    self.client_ob = new_client
                    self.last_reconnect_ts_ob = time.time()
                    self.is_reconnecting_ob = False
                    self.last_msg_time_ob = time.time()
                else:
                    self.client_trade = new_client
                    self.last_reconnect_ts_trade = time.time()
                    self.is_reconnecting_trade = False
                    self.last_msg_time_trade = time.time()
                ws_reconnect_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
            except Exception as e:
                self.logger.error(f"❌ reset client have faield: {e}")
            finally:
                self.is_reconnecting_ob = False
                self.is_reconnecting_trade = False

    async def watchdog(self,client_type='ob'):
        self.logger.info("🛡 Watchdog started")
        while self.is_running:
            await asyncio.sleep(5)
            reconnecting_flag = self.is_reconnecting_ob if client_type == 'ob' else self.is_reconnecting_trade
            if reconnecting_flag: continue
            last_msg_time = self.last_msg_time_ob if client_type == 'ob' else self.last_msg_time_trade
            silence = time.time() - last_msg_time
            silence_gauge.labels(
                exchange=self.exchange_id,
                symbol=self.symbol,
                type=client_type
            ).set(silence)
            queue_size_gauge.labels(
                exchange=self.exchange_id,
                symbol=self.symbol
            ).set(self.queue.qsize())
            if silence > 60:
                self.logger.error(f"🚨[{client_type}] WS silent for {silence:.1f}s, force reconnect")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
                if client_type == 'ob':
                    await self._reset_client(client_type='ob')
                else:
                    await self._reset_client(client_type='trade')

    async def push_metrics_periodically(self):
        self.logger.info("🛡 Metrics pusher started")
        while self.is_running:
            try:
                await asyncio.to_thread(
                    push_to_gateway,
                    'http://pushgateway:9091',
                    job="binance_collector_v1",
                    registry=REGISTRY,
                    timeout=3
                )
                
                await asyncio.sleep(10)
            except Exception as e:
                self.logger.error(f"Push metrics failed: {e}")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
                await asyncio.sleep(10)

    async def _backfill_trade(self,start_id:int,end_id:int):

        self.logger.info(f"⏳ [BACKFILL][{self.symbol}] 启动反补自愈: ID {start_id} -> {end_id}")
        current_from_id = start_id
        total_patched = 0
        try:
            while current_from_id <= end_id:
                params = {
                    'symbol':self.symbol.replace('/','').replace('-',''),
                    'fromId':current_from_id,
                    'limit':1000
                }
                
                try:
                    trades = await asyncio.wait_for(
                        self.client_rest.publicGetHistoricalTrades(params),
                        timeout=20
                    )
                except asyncio.TimeoutError:
                    asyncio.sleep(30)
                    continue
                
                for trade in trades:
                    t_id = int(trade['id'])
                    if t_id > end_id:
                        current_from_id = end_id + 1
                        break

                    if await self.r.setnx(f"{self.dedup_prefix}:{t_id}", 1):
                        await self.r.expire(f"{self.dedup_prefix}:{t_id}", 3600)

                        ccxt_format = {
                            'symbol': self.symbol,
                            'id': str(trade['id']),
                            'timestamp': int(trade['time']),
                            'side': 'sell' if trade['isBuyerMaker'] else 'buy',
                            'price': float(trade['price']),
                            'amount': float(trade['qty']),
                            'cost': float(trade['quoteQty']),
                            'is_taker_buyer': False if trade['isBuyerMaker'] else True,
                            'info': trade # 原始数据存入 info
                        }

                        trade_obj = TradeData.from_ccxt(ccxt_format, self.exchange_id)

                        await self.queue.put(trade_obj)

                        current_from_id = t_id + 1
                        total_patched += 1

                self.logger.info(f"✅ [BACKFILL][{self.symbol}] 已补全 {total_patched} 条数据，当前进度 ID: {current_from_id}")
                if len(trades) < 1000:
                    current_from_id = end_id + 1 # 强制跳出 while
                    break
                
                # 防止触发 API 频率限制
                await asyncio.sleep(1)
        except Exception as e:
            self.logger.error(f"❌ [BACKFILL-CRITICAL] 反补过程中断: {e}")
                
    async def stop(self):
        await self.client_ob.close()
        await self.client_trade.close()
        await super().stop()
                
