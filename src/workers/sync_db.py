import clickhouse_connect
import os
import time
from dotenv import load_dotenv
from src.utils.logger import setup_logger
from clickhouse_connect.driver.exceptions import DatabaseError

load_dotenv()

class SyncDB:
    def __init__(self):
        self.local_client = None
        self.remote_client = None
        self.logger = setup_logger(f"collector.ws.okx",log_file=f"logs/collector/collector_okx.log")

    def _connect_ch_db(self):
        """带有异常捕获的重连机制"""
        try:
            # 建立本地连接
            self.local_client = clickhouse_connect.get_client(
                host=os.getenv('CLICKHOUSE_HOST'),
                port=os.getenv('CLICKHOUSE_PORT'),
                username=os.getenv('CLICKHOUSE_USERNAME'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
                database=os.getenv('CLICKHOUSE_DB')
            )
            # 建立香港远程连接
            self.remote_client = clickhouse_connect.get_client(
                host=os.getenv('HK_HOST'),
                port=os.getenv('CLICKHOUSE_PORT'),
                username=os.getenv('CLICKHOUSE_USERNAME'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
                database=os.getenv('CLICKHOUSE_DB')
            )
            return True
        except Exception as e:
            self.logger.error(f"🚨 连接初始化失败: {e}")
            return False

    def get_hk_db(self, table):
        if table == 'orderbook':
            max_target, cols = 'nonce', "nonce,symbol,exchange_id,mkt_type,bid_volume,bid_price,ask_volume,ask_price,bid_prices,bid_volumes,ask_prices,ask_volumes,timestamp,local_timestamp"
        elif table == 'trades':
            max_target, cols = 'trade_id', "trade_id,trade_id_raw,symbol,exchange_id,mkt_type,price,amount,side,is_taker_buyer,timestamp,local_timestamp"

        # 每次进入表同步前确保连接正常
        if not self._connect_ch_db():
            return

        sql = f"""
            SELECT exchange_id, mkt_type, symbol, MAX(toInt64({max_target})) AS {max_target}
            FROM market_data.{table}
            WHERE exchange_id='okx'
            GROUP BY exchange_id, mkt_type, symbol
        """
        
        try:
            local_result = self.local_client.query(sql)
        except Exception as e:
            self.logger.error(f"❌ 本地查询任务清单失败: {e}")
            return
        
        if local_result.result_rows:
            for row in local_result.result_rows:
                exchange_id, mkt_type, symbol, local_id = row
                chunk_size = 10000

                while True:
                    try:
                        remote_sql = f"""
                            SELECT {cols} FROM {table} 
                            WHERE toInt64({max_target}) > {local_id}
                            AND exchange_id='{exchange_id}' AND symbol='{symbol}' AND mkt_type='{mkt_type}'
                            ORDER BY toInt64({max_target}) ASC LIMIT {chunk_size}
                        """
                        remote_result = self.remote_client.query_arrow(remote_sql)

                        if remote_result.num_rows == 0:
                            self.logger.info(f"🏁 {exchange_id} {symbol} {table} 已达到实时位置。")
                            break
                        
                        self.local_client.insert_arrow(table,remote_result)
                        new_local_id = int(remote_result.column(0)[-1].as_py())
                        
                        if new_local_id <= local_id:
                            break
                                
                        local_id = new_local_id
                        self.logger.info(f"🚀 {symbol} 搬运中... 进度: {local_id} | 规模: {remote_result.num_rows}")
                        
                    except (DatabaseError, Exception) as e:
                        self.logger.warning(f"⚠️ 遇到网络抖动: {e}，休息30秒后重连...")
                        time.sleep(30)
                        self._connect_ch_db()
                        continue

    def run_forever(self):
        """永动机主循环"""
        print("💡 量化数据同步永动机已启动...")
        while True:
            try:
                start_time = time.time()
                
                # 依次同步两张表
                for table in ['trades', 'orderbook']:
                    self.get_hk_db(table)
                
                duration = time.time() - start_time
                wait_time = 60  # 每轮跑完休息 1 分钟，避免高频请求被防火墙拉黑
                print(f"☕ 本轮同步耗时 {duration:.2f}s，{wait_time}s 后开始下一轮巡检...")
                time.sleep(wait_time)
                
            except KeyboardInterrupt:
                self.logger.info("🛑 CEO 手动停止了永动机。")
                break
            except Exception as e:
                self.logger.error(f"🔥 顶层意外崩溃: {e}，5分钟后尝试总重启...")
                time.sleep(300)

if __name__ == '__main__':
    sync_db = SyncDB()
    sync_db.run_forever()