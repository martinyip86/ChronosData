import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

class ClickhouseManager:
    def __init__(self):
        self._ch_client = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST'),
            port=os.getenv('CLICKHOUSE_PORT'),
            username=os.getenv('CLICKHOUSE_USERNAME'),
            password=os.getenv('CLICKHOUSE_PASSWORD'),
            database=os.getenv('CLICKHOUSE_DB')
        )

    @property
    def market_db(self):
        return self._ch_client
    
ch_manager = ClickhouseManager()