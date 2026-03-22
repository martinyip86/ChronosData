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
        # Updated log path and logger name to English
        self.logger = setup_logger(f"collector.ws.okx", log_file=f"logs/collector/collector_okx.log")

    def _connect_ch_db(self):
        """Reconnection mechanism with exception handling"""
        try:
            # Establish local connection
            self.local_client = clickhouse_connect.get_client(
                host=os.getenv('CLICKHOUSE_HOST'),
                port=os.getenv('CLICKHOUSE_PORT'),
                username=os.getenv('CLICKHOUSE_USERNAME'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
                database=os.getenv('CLICKHOUSE_DB')
            )
            # Establish remote connection (Hong Kong)
            self.remote_client = clickhouse_connect.get_client(
                host=os.getenv('HK_HOST'),
                port=os.getenv('CLICKHOUSE_PORT'),
                username=os.getenv('CLICKHOUSE_USERNAME'),
                password=os.getenv('CLICKHOUSE_PASSWORD'),
                database=os.getenv('CLICKHOUSE_DB')
            )
            return True
        except Exception as e:
            self.logger.error(f"🚨 Connection initialization failed: {e}")
            return False

    def get_hk_db(self, table):
        if table == 'orderbook':
            max_target, cols = 'nonce', "nonce,symbol,exchange_id,mkt_type,bid_volume,bid_price,ask_volume,ask_price,bid_prices,bid_volumes,ask_prices,ask_volumes,timestamp,local_timestamp"
        elif table == 'trades':
            max_target, cols = 'trade_id', "trade_id,trade_id_raw,symbol,exchange_id,mkt_type,price,amount,side,is_taker_buyer,timestamp,local_timestamp"

        # Ensure connections are active before starting table sync
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
            self.logger.error(f"❌ Failed to query local task list: {e}")
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
                            self.logger.info(f"🏁 {exchange_id} {symbol} {table} is now up to date.")
                            break
                        
                        self.local_client.insert_arrow(table, remote_result)
                        new_local_id = int(remote_result.column(0)[-1].as_py())
                        
                        if new_local_id <= local_id:
                            break
                                
                        local_id = new_local_id
                        self.logger.info(f"🚀 {symbol} Syncing... Progress: {local_id} | Batch Size: {remote_result.num_rows}")
                        
                    except (DatabaseError, Exception) as e:
                        self.logger.warning(f"⚠️ Network jitter detected: {e}. Retrying in 30s...")
                        time.sleep(30)
                        self._connect_ch_db()
                        continue

    def run_forever(self):
        """Main infinite loop for synchronization"""
        print("💡 Quant Data Sync Engine started...")
        while True:
            try:
                start_time = time.time()
                
                # Sync tables sequentially
                for table in ['trades', 'orderbook']:
                    self.get_hk_db(table)
                
                duration = time.time() - start_time
                wait_time = 60  # 1-minute cooldown to prevent firewall rate-limiting
                print(f"☕ Round completed in {duration:.2f}s. Next inspection in {wait_time}s...")
                time.sleep(wait_time)
                
            except KeyboardInterrupt:
                self.logger.info("🛑 Engine stopped manually by user.")
                break
            except Exception as e:
                self.logger.error(f"🔥 Critical crash: {e}. Attempting full restart in 5 minutes...")
                time.sleep(300)

if __name__ == '__main__':
    sync_db = SyncDB()
    sync_db.run_forever()