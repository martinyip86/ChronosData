import asyncio
import argparse
import importlib
from src.collectors.providers.binance import BinanceStream
from src.utils.logger import setup_logger
from src.storage.redis_client import redis_manager
from src.storage.state_watcher import StateWatcher
from prometheus_client import push_to_gateway,REGISTRY

class StreamCommander:
    """
    Market Data Orchestrator.
    Manages the lifecycle of multiple exchange collectors, handling parallel 
    execution, resource allocation, and Prometheus metric pushing.
    """
    def __init__(self,target_exchange=None):
        # Initialize logger with exchange-specific scoping
        self.logger = setup_logger("manager",log_file=f"logs/collector/collector_{target_exchange}.log")

        # Define ingestion scope
        if target_exchange is not None:
            self.exchanges = [target_exchange]
        else:
            self.exchanges = ['binance','okx']

        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT']
        self.data_types = ['orderbook','trades']
        self.running_tasks = []

    async def run(self):
        """
        Starts the parallel ingestion matrix and monitoring tasks.
        """
        redis_client = redis_manager.market_db
        self.logger.info("🇨🇭 [SYSTEM] Initializing parallel ingestion matrix...")

        # State Recovery & Sequence Synchronization
        watcher_tak = asyncio.create_task(StateWatcher.run(interval=15))
        self.running_tasks.append(watcher_tak)

        await asyncio.sleep(2)
        try:
            for exchange_name in self.exchanges:

                for symbol in self.symbols:
                    for type_name in self.data_types:
                        self.logger.info(f"🚀 [SCHEDULE] Deploying task: {exchange_name} | {symbol} | {type_name}")

                        # Initialize collector instance
                        collector = BinanceStream(exchange=exchange_name,symbol=symbol, redis_client=redis_client,dtype=type_name)

                        # Wrap in safe_run to prevent a single task failure from crashing the entire matrix
                        task = asyncio.create_task(self.safe_run(collector, exchange_name, symbol, type_name))
                        self.running_tasks.append(task)

            # Start periodic metrics push to Prometheus Gateway
            asyncio.create_task(self.push_metrics_periodically())

            if self.running_tasks:
                self.logger.info("⚡ [EXECUTE] Matrix operational. Processing event loops...")
                await asyncio.gather(*self.running_tasks)
            else:
                self.logger.warning("⚠️ [WARN] No tasks scheduled. Please check configuration.")
        except Exception as e:
            self.logger.error(f"💥 [CRITICAL] Orchestrator crashed: {e}", exc_info=True)
        finally:
            # Graceful shutdown: cancel all coroutines and release resources
            self.logger.info("🛑 [SHUTDOWN] Initiating graceful shutdown...")
            for t in self.running_tasks:
                t.cancel()
            await redis_client.close()
            self.logger.info("🏁 [EXIT] All resources released.")

    async def safe_run(self, collector, ex, sym, typ):
        try:
            await collector.run()
        except Exception as e:
            self.logger.error(f"❌ [TASK ERROR] {ex}-{sym}-{typ} terminated unexpectedly: {e}", exc_info=True)

    async def push_metrics_periodically(self):
        """
        Background worker to push local metrics to Prometheus Pushgateway.
        """
        while True:
            try:
                await asyncio.to_thread(
                    push_to_gateway,
                    'http://pushgateway:9091',
                    job="market_collector",
                    registry=REGISTRY
                )
            except: pass
            await asyncio.sleep(10)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hydra-Feed Stream Orchestrator")
    parser.add_argument('--exchange',type=str,help='Target exchange (e.g., binance/okx)')
    args = parser.parse_args()
    
    stream_commander = StreamCommander(target_exchange=args.exchange)
    try:
        asyncio.run(stream_commander.run())
    except KeyboardInterrupt:
        pass