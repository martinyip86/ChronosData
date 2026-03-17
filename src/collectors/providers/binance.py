import asyncio
import ccxt.pro as ccxt_pro
import time
import json
from src.collectors.base_stream import BaseStream
from src.storage.ch_client import ch_manager
from src.models.schema import TickData,TradeData
from prometheus_client import push_to_gateway,REGISTRY,generate_latest
from src.monitoring.metrics import ws_reconnect_total,ws_error_total,silence_gauge,parquet_write_duration,queue_size_gauge

class BinanceStream(BaseStream):
    def __init__(self,exchange,symbol,redis_client,dtype):
        super().__init__(exchange,symbol,redis_client,dtype)
        self.client = None
        self.mkt_type = 'spot'
        self.last_trade_id_key = f"last_trade_id:{self.exchange_id}:{self.symbol}"
        self.dedup_prefix = f"seen:{self.exchange_id}:{self.symbol}"
        self.last_id_mem = None

    def _create_client(self):
        config = {
            'enableRateLimit':True,
            'options':{'defaultType':self.mkt_type}
        }
        if self.exchange_id == 'binance':
            return ccxt_pro.binance(config)
        elif self.exchange_id == 'okx':
            return ccxt_pro.okx(config)

    async def connect(self):
        if self.client:
            await self.client.close()

        self.client = self._create_client()
        self.first_message_received = False

        try:
            if self.dtype == 'orderbook':
                await self._watch_orderbook()
            else:
                await self._watch_trade()
        finally:
            await self.client.close()
            del self.client
            self.client = None


    async def _watch_orderbook(self):
        last_active_time = time.time()

        while not self._stop_event.is_set():
            try:
                orderbook = await asyncio.wait_for(
                    self.client.watch_order_book(self.symbol),
                    timeout=20
                )

                if not self.first_message_received:
                    self.logger.info(f"✅ [RECONNECTED] {self.exchange_id}-{self.symbol}-orderbook 链路已激活，收到首批数据。")
                    # 可以在这里更新 Prometheus 指标
                    silence_gauge.labels(
                        exchange=self.exchange_id,
                        symbol=self.symbol,
                        type='orderbook'
                    ).set(0)
                    self.first_message_received = True

                last_active_time = time.time()
                silence_gauge.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol,
                    type='orderbook'
                ).set(0)
                
                raw_ts = orderbook.get('timestamp')
                ts = raw_ts if raw_ts is not None else int(time.time() * 1000)
                top_20_bids = orderbook['bids'][:20]
                top_20_asks = orderbook['asks'][:20]
                tick = TickData(
                    symbol=self.symbol,
                    exchange_id=self.exchange_id,
                    mkt_type=self.mkt_type,
                    bid_price=orderbook['bids'][0][0],
                    bid_volume=orderbook['bids'][0][1],
                    ask_price=orderbook['asks'][0][0],
                    ask_volume=orderbook['asks'][0][1],
                    bid_prices=[row[0] for row in top_20_bids],
                    bid_volumes=[row[1] for row in top_20_bids],
                    ask_prices=[row[0] for row in top_20_asks],
                    ask_volumes=[row[1] for row in top_20_asks],
                    nonce=orderbook['nonce'],
                    timestamp=int(ts),
                    exchange_time=int(ts)
                )
                await self.redis.rpush("market:ticks:all",tick.model_dump_json())
                # self.logger.info(f"📊 [OB-PRODUCER] {self.symbol} | 已写入| 实时价格: {tick.bid_price}")
            except asyncio.TimeoutError:
                self.first_message_received = False
                self.logger.warning(f"{self.exchange_id}:{self.symbol}:orderbook_ws timeout,reconnecting...")
                ws_reconnect_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()

                silence_duration = time.time() - last_active_time
                self.logger.warning(f"🤫 {self.exchange_id}-{self.symbol} 静默中: {silence_duration:.1f}s")
                
                # 更新指标
                silence_gauge.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol,
                    type='orderbook'
                ).set(silence_duration)

                if silence_duration > 60:
                    raise ConnectionError(f"Zombie connection detected for {self.exchange_id}-{self.symbol}")
                    
            except Exception as e:
                self.logger.error(f"🚨 [OB-ERROR] listener Exception: {e}")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
                raise e
            

    async def _watch_trade(self):
        # last_id = await self.redis.get(self.last_trade_id_key)
        # if not last_id:
        #     try:
        #         # 建议：由于 ClickHouse 查询是同步 IO，建议放在 to_thread 里跑，防止卡死其他协程
        #         ch_client = ch_manager.market_db
        #         sql = f"""
        #             SELECT MAX(trade_id) FROM trades 
        #             WHERE exchange_id='{self.exchange_id}' AND symbol='{self.symbol}'
        #         """
        #         # 使用 to_thread 防止查询时的网络延迟卡住整个采集器
        #         result = await asyncio.to_thread(ch_client.query, sql)

        #         if result.result_rows and result.result_rows[0][0] is not None:
        #             last_id = result.result_rows[0][0]
        #         else:
        #             last_id = None
        #     except Exception as e:
        #         self.logger.error(f"❌ 从 ClickHouse 初始化 last_id 失败: {e}")
        #         last_id = None
        #         raise e
        #     finally:
        #         ch_client.close()
        redis_hex_trade_id_key = f"cache:{self.exchange_id}:last_trade_id"
        last_id = await self.redis.hget(redis_hex_trade_id_key,self.symbol)
        if last_id:
            self.last_id_mem = int(last_id)
            self.logger.info(f"📥 [INIT] {self.exchange_id}-{self.symbol} 从 Redis 恢复起点: {self.last_id_mem}")
        else:
            self.logger.warning(f"⚠️ [INIT] Redis 中未找到 {self.exchange_id}-{self.symbol} 的状态，将从实时流首条数据开始。")
            self.last_id_mem = None
        last_active_time = time.time()

        while not self._stop_event.is_set():
            try:
                trades_list = await asyncio.wait_for(
                    self.client.watch_trades(self.symbol),
                    timeout=20
                )

                if not self.first_message_received:
                    self.logger.info(f"✅ [RECONNECTED] {self.exchange_id}-{self.symbol}-trades 链路已激活，收到首批数据。")
                    # 可以在这里更新 Prometheus 指标
                    silence_gauge.labels(
                        exchange=self.exchange_id,
                        symbol=self.symbol,
                        type='trade'
                    ).set(0)
                    self.first_message_received = True

                last_active_time = time.time()
                silence_gauge.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol,
                    type='trade'
                ).set(0)

                batch_data = []
                max_curr_id = self.last_id_mem

                for trade_dict in trades_list:
                    trade = TradeData.from_ccxt(trade_dict,self.exchange_id,self.mkt_type)

                    curr_id = int(trade.trade_id)

                    if self.last_id_mem is not None:
                        if curr_id > self.last_id_mem + 1:
                            gap_job = {
                                'symbol':self.symbol,
                                'exchange_id':self.exchange_id,
                                'start_id':self.last_id_mem + 1,
                                'end_id':curr_id - 1
                            }
                            await self.redis.rpush("queue:gap_fill_jobs",json.dumps(gap_job))
                            self.logger.warning(f"🕳️ [GAP] {self.exchange_id}-{self.symbol} 发现空洞: {self.last_id_mem} -> {curr_id}")

                    if self.last_id_mem is None or curr_id > self.last_id_mem:
                        self.last_id_mem = curr_id
                        max_curr_id = curr_id

                    batch_data.append(trade.model_dump_json())

                if batch_data:
                    #后补即时反补函数
                    await self.redis.rpush(f"market:trades:all",*batch_data)
                    await self.redis.hset(redis_hex_trade_id_key, self.symbol, max_curr_id)
                    await self.redis.set(self.last_trade_id_key,max_curr_id)

            except asyncio.TimeoutError:
                self.first_message_received = False
                self.logger.warning(f"{self.exchange_id}:{self.symbol}:trade_ws timeout,reconnecting...")
                ws_reconnect_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()

                silence_duration = time.time() - last_active_time
                self.logger.warning(f"🤫 {self.symbol} 静默中: {silence_duration:.1f}s")
                
                # 更新指标
                silence_gauge.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol,
                    type='trade'
                ).set(silence_duration)
                
                if silence_duration > 60:
                    raise ConnectionError(f"Zombie connection detected for {self.exchange_id}-{self.symbol}")

            except Exception as e:
                self.logger.error(f"🚨 [TD-ERROR] listener Exception: {e}")
                ws_error_total.labels(
                    exchange=self.exchange_id,
                    symbol=self.symbol
                ).inc()
                raise e
            