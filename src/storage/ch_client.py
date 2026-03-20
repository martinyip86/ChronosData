import clickhouse_connect
import os
from dotenv import load_dotenv

# Load environment variables from .env file for secure credential management
load_dotenv()

class ClickhouseManager:
    """
    ClickHouse Infrastructure Manager.
    Handles the lifecycle of the OLAP database connection using a Singleton-like 
    Lazy Loading pattern to ensure efficient resource utilization.
    """
    def __init__(self):
        # Internal client state, initialized only on first access
        self._ch_client = None

    @property
    def market_db(self):
        """
        Returns an active ClickHouse client instance.
        Initializes the connection if it doesn't exist (Lazy Initialization).
        """
        if self._ch_client is None:
            # Retrieve connection parameters from environment variables
            host = os.getenv('CLICKHOUSE_HOST', 'clickhouse-server')
            port = os.getenv('CLICKHOUSE_PORT', '8123')
            username = os.getenv('CLICKHOUSE_USERNAME', 'default')
            password = os.getenv('CLICKHOUSE_PASSWORD', '')
            database = os.getenv('CLICKHOUSE_DB', 'market_data')
            try:
                self._ch_client = clickhouse_connect.get_client(
                    host=host,
                    port=int(port),
                    username=username,
                    password=password,
                    database=database,
                )
                print(f"✅ [DATABASE] ClickHouse connection established: {host}:{port}")
            except Exception as e:
                # Critical failure: Ensure the system crashes early if DB is unreachable
                print(f"❌ [DATABASE-ERROR] Failed to connect to ClickHouse: {e}")
                raise e

        return self._ch_client

# Global instance for project-wide access
ch_manager = ClickhouseManager()