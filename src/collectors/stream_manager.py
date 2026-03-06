import asyncio
import argparse
import importlib
from src.utils.logger import setup_logger
from src.storage.redis_client import redis_manager

logger = setup_logger("manager")

async def main():
    # 1. 命令行参数解析
    parser = argparse.ArgumentParser(description="量化数据采集总指挥")
    parser.add_argument("--exchange",type=str,required=True,help="交易所名称 (如: binance, okx)")
    parser.add_argument("--symbol",type=str,required=True,help="交易对 (如: BTC/USDT)")
    parser.add_argument("--type",type=str,required=True,help="orderbook,trade")
    args = parser.parse_args()

    exchange_name = args.exchange.lower()
    symbol = args.symbol.upper()
    type_name = args.type.lower()

    redis_client = redis_manager.local_db

    try:
        module_path = f"src.collectors.providers.{exchange_name}"
        try:
            provider_module = importlib.import_module(module_path)
        except Exception as e:
            logger.error(f"❌ 找不到交易所插件: {module_path}. 请检查 providers 目录下是否有该文件。")
            return

        class_name = f"{exchange_name.capitalize()}Stream"
        if not hasattr(provider_module,class_name):
            logger.error(f"❌ 模块 {module_path} 中没有找到类 {class_name}")
            return
        
        stream_class = getattr(provider_module,class_name)
        logger.info(f"🚀 [INIT] 启动 {class_name} | 交易对: {symbol}")

        collector = stream_class(symbol=symbol, redis_client=redis_client,dtype=type_name)
        await collector.run()
    except Exception as e:
        logger.error(f"💥 [CRITICAL] 运行崩溃: {e}", exc_info=True)
    finally:
        await redis_client.close()
        logger.info(f"🏁 [EXIT] {exchange_name} 采集进程已退出")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass