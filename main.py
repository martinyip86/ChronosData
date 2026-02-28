import asyncio
from prometheus_client import start_http_server
from src.collectors.binance_ws import BinanceController
from src.monitoring import metrics

# async def main():
#     start_http_server(8000)

#     symbol = 'BTC/USDT'
#     controller = BinanceController(symbol)

#     await controller.connect()
#     try:
#         print("\n正在开始采集程序...")
#         await asyncio.gather(
#             controller.fetch_data(),
#             controller.storage_worker()
#         )
#     except asyncio.CancelledError:
#         print("\n正在停止采集程序...")
#     finally:
#         await controller.stop()

async def main():
    start_http_server(8000)

    symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT']
    controllers = [BinanceController(symbol) for symbol in symbols]

    tasks = []

    for controller in controllers:
        await controller.connect()
        tasks.append(run_single_controller(controller))

    try:
        print("\n正在开始采集程序...")
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        print("\n正在停止采集程序...")
    finally:
        for controller in controllers:
            await controller.stop()

async def run_single_controller(controller):
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