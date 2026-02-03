import asyncio
from src.collectors.binance_ws import BinanceController

async def main():
    symbol = 'BTC/USDT'
    controller = BinanceController(symbol)

    await controller.connect()

    try:
        print("\n正在开始采集程序...")
        await asyncio.gather(
            controller.fetch_data(),
            controller.storage_worker()
        )
    except asyncio.CancelledError:
        print("\n正在停止采集程序...")
    finally:
        await controller.stop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass