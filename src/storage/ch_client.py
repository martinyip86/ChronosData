import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

class ClickhouseManager:
    def __init__(self):
        self._ch_client = None

    @property
    def market_db(self):
        if self._ch_client is None:
            host = os.getenv('CLICKHOUSE_HOST', 'clickhouse-server')
            try:
                self._ch_client = clickhouse_connect.get_client(
                    host=host,
                    port=int(os.getenv('CLICKHOUSE_PORT', 8123)),
                    username=os.getenv('CLICKHOUSE_USERNAME', 'default'),
                    password=os.getenv('CLICKHOUSE_PASSWORD', 'martin1510'),
                    database=os.getenv('CLICKHOUSE_DB', 'market_data'),
                )
                print("✅ ClickHouse 连接成功")
            except Exception as e:
                print(f"❌ ClickHouse 连接失败: {e}")
                raise e

        return self._ch_client
    
ch_manager = ClickhouseManager()