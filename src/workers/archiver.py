import polars as pl
import os
import uuid
import sys
import gc
from datetime import datetime,timedelta,timezone
from src.storage.ch_client import ch_manager
from src.utils.logger import setup_logger

class Archiver():
    def __init__(self,target_date:str):
        self.ch_client = None
        self.logger = setup_logger('workers.archive')
        self.target_date = target_date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        self.fields = {
            'orderbook':"nonce,symbol,exchange_id,bid_price,bid_volume,ask_price,ask_volume,bid_prices,bid_volumes,ask_prices,ask_volumes,timestamp",
            'trades':"trade_id,symbol,exchange_id,price,amount,side,is_taker_buyer,timestamp"
        }
        self.sort_keys = {
            'orderbook': 'nonce',
            'trades': 'trade_id'
        }
        self.exchanges = ['binance']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT']
        self.data_types = ['orderbook','trades']

    def setup(self):
        self.ch_client = ch_manager.market_db

    def get_ch_data(self,table,exchange_id,symbol,hour):
        field = self.fields[table]
        sort_key = self.sort_keys[table]
        sql = f"""
            SELECT
                {field}
            FROM
                {table} FINAL
            WHERE timestamp >= toUnixTimestamp64Milli(toDateTime64('{self.target_date} {hour:02d}:00:00',3 ,'UTC'))
                AND timestamp < toUnixTimestamp64Milli(toDateTime64('{self.target_date} {hour:02d}:00:00',3 ,'UTC') + INTERVAL 1 HOUR)
                AND exchange_id = '{exchange_id}'
                AND symbol = '{symbol}'
            ORDER BY {sort_key}
        """
        self.logger.info(f"💾 正在提取 {table} 数据，日期: {self.target_date}")
        result = self.ch_client.query(sql)
        if not result.result_rows:
            return pl.DataFrame()

        df = pl.DataFrame(
            data=result.result_rows,
            schema=result.column_names,
            orient='row'
        )

        return df

    def export_parquet(self,df:pl.DataFrame,dtype,exchange_id,symbol,hour):
        date_obj = datetime.strptime(self.target_date,'%Y-%m-%d')

        if df.is_empty():
            self.logger.warning(f"⚠️ {dtype} 数据集为空，跳过归档")
            return

        clear_symbol = symbol.replace('/','-')
        dir_path = os.path.join(
            f"data/raw/{exchange_id}/spot/{clear_symbol}/{dtype}",
            date_obj.strftime('%Y'),
            date_obj.strftime('%m'),
            date_obj.strftime('%d')
        )
        os.makedirs(dir_path,exist_ok=True)
        filename = f"{dtype}_{date_obj.strftime('%Y%m%d')}_{hour:02d}.parquet"
        file_path = os.path.join(dir_path,filename)
        if not os.path.exists(file_path):
            temp_path = f"{file_path}.{uuid.uuid4().hex}.tmp"
            try:
                df.write_parquet(temp_path,compression='snappy')
                os.replace(temp_path,file_path)
                self.logger.info(f"✅ [{exchange_id}-{symbol}-{dtype}-({self.target_date})] 写入成功 总数: {len(df)}")
            except Exception as e:
                self.logger.error(f"❌ [{exchange_id}-{symbol}-{dtype}-({self.target_date})] 冷数据保存失败: {e}")

            finally:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                
        del df
                

    def run(self):
        self.setup()

        for table in self.data_types:
            for exchange_id in self.exchanges:
                for symbol in self.symbols:
                    for hour in range(24):
                        self.logger.info(f"⏳ 正在提取 {symbol} {table} - {hour:02d} 时段")
                        try:
                            df = self.get_ch_data(table=table,exchange_id=exchange_id,symbol=symbol,hour=hour)
                            self.export_parquet(df=df, dtype=table,exchange_id=exchange_id,symbol=symbol,hour=hour)
                        except Exception as e:
                            self.logger.error(f"💥 {table} 归档流程异常: {e}")
                            raise

                        gc.collect()

def archiver(target_date: str=None):
    # custom_date = sys.argv[1] if len(sys.argv) > 1 else target_date
    archiver_obj = Archiver(target_date=target_date)
    archiver_obj.run()

if __name__ == '__main__':
    archiver()
    

        