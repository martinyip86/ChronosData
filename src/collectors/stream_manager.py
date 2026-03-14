import asyncio
import argparse
import importlib
from src.collectors.providers.binance import BinanceStream
from src.utils.logger import setup_logger
from src.storage.redis_client import redis_manager
from src.storage.state_watcher import StateWatcher
from prometheus_client import push_to_gateway,REGISTRY

class StreamCommander:
    def __init__(self):
        self.logger = setup_logger("manager",log_file="logs/collector/collector.log")
        self.exchanges = ['binance','okx']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT']
        self.data_types = ['orderbook','trades']
        self.running_tasks = []

    async def run(self):
        redis_client = redis_manager.market_db
        self.logger.info("🇨🇭 [SYSTEM] 启动并行采集矩阵...")
        watcher_tak = asyncio.create_task(StateWatcher.run(interval=15))
        self.running_tasks.append(watcher_tak)

        await asyncio.sleep(2)
        try:
            for exchange_name in self.exchanges:
                # module_path = f"src.collectors.providers.{exchange_name}"
                # try:
                #     provider_module = importlib.import_module(module_path)
                # except Exception as e:
                #     self.logger.error(f"❌ 找不到交易所插件: {module_path}. 请检查 providers 目录下是否有该文件。")
                #     return

                # class_name = f"{exchange_name.capitalize()}Stream"
                # if not hasattr(provider_module,class_name):
                #     self.logger.error(f"❌ 模块 {module_path} 中没有找到类 {class_name}")
                #     return
                
                # stream_class = getattr(provider_module,class_name)

                for symbol in self.symbols:
                    for type_name in self.data_types:
                        self.logger.info(f"🚀 [SCHEDULE] 调度任务: {exchange_name} | {symbol} | {type_name}")
                        collector = BinanceStream(exchange=exchange_name,symbol=symbol, redis_client=redis_client,dtype=type_name)
                        task = asyncio.create_task(self.safe_run(collector, exchange_name, symbol, type_name))
                        self.running_tasks.append(task)

            asyncio.create_task(self.push_metrics_periodically())

            if self.running_tasks:
                self.logger.info("开始执行协程...")
                await asyncio.gather(*self.running_tasks)
            else:
                self.logger.warning("⚠️ 没有任务被启动，请检查配置。")
        except Exception as e:
            self.logger.error(f"💥 [CRITICAL] 运行崩溃: {e}", exc_info=True)
        finally:
            # 尝试优雅关闭所有任务
            for t in self.running_tasks:
                t.cancel() # 取消协程
            await redis_client.close()
            self.logger.info("🏁 [EXIT] 采集资源已释放")

    async def safe_run(self, collector, ex, sym, typ):
        try:
            await collector.run()
        except Exception as e:
            self.logger.error(f"❌ 任务 {ex}-{sym}-{typ} 意外终止: {e}", exc_info=True)

    async def push_metrics_periodically(self):
        while True:
            try:
                await asyncio.to_thread(
                    push_to_gateway,
                    'http://pushgateway:9091',
                    job="market_collector",  # 整个采集矩阵作为一个 job
                    registry=REGISTRY
                )
            except: pass
            await asyncio.sleep(10)

if __name__ == "__main__":
    stream_commander = StreamCommander()
    try:
        asyncio.run(stream_commander.run())
    except KeyboardInterrupt:
        pass