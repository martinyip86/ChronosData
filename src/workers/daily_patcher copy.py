import polars as pl
import os
import io
import gc
import requests
import time
import zipfile
from datetime import datetime,timedelta,timezone
from src.storage.ch_client import ch_manager
from src.utils.logger import setup_logger
from src.processors.validator import validator_trades

class DailyPatcher:
    def __init__(self,target_date:str):
        self.date_str = target_date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        self.logger = setup_logger('daily.patcher',log_file='logs/syncer/daily_patcher.log')
        self.logger.propagate = False
        self.ch_client = None
        self.exchange_ids = ['binance','okx']
        self.symbols = ['BTC/USDT','ETH/USDT','PEPE/USDT','SOL/USDT']
        self.schema = []
        self.csv_columns = {
            'binance': ["trade_id","price","amount","cost","timestamp","is_maker","is_best"],
            'okx': ["symbol","trade_id","side","price","amount","timestamp"]
        }

    def setup(self):
        self.ch_client = ch_manager.market_db

    def check_data_exists(self,target_date_str:str,exchange_id,symbol):
        sql = f"""
            SELECT 
                trade_id 
            FROM 
                trades 
            WHERE symbol='{symbol}' AND exchange_id='{exchange_id}'
                AND timestamp >= toUnixTimestamp64Milli(toDateTime64('{target_date_str} 00:00:00',3))
                AND timestamp < toUnixTimestamp64Milli(toDateTime64('{target_date_str} 00:00:00',3) + INTERVAL 1 DAYS)
            LIMIT 1
        """
        result = self.ch_client.query(sql)
        if not result.result_rows:
            self.logger.warning(f"🚨 {exchange_id} {symbol} 在 {target_date_str} 数据严重缺失，启动全量同步")
            file_path = self.download_and_unzip(exchange_id,symbol,target_date_str)
            if file_path and os.path.exists(file_path):
                try:
                    official_df = self._changeColumns(exchange_id,symbol,file_path)
                    validator_trades(official_df)
                    self.sync_to_clickhouse(exchange_id,symbol,official_df)
                    self.logger.info(f"✅ {exchange_id} {symbol} 补丁同步完成: 缝合 {len(official_df)} 条数据")

                    del official_df
                    return True
                except Exception as e:
                    self.logger.error(f"❌ 处理 {exchange_id} {symbol} 补丁时出错: {e}")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)

        return False

    def main(self):
        self.setup()
        table_schema = self.ch_client.query("DESCRIBE TABLE trades")
        valid_columns = table_schema.result_rows
        self.schema = [row[0] for row in valid_columns if row[0] != "created_at"]

        for exchange_id in self.exchange_ids:
            for symbol in self.symbols:
                flag = self.check_data_exists(self.date_str,exchange_id,symbol)
                gc.collect()
                if flag: continue

                gaps = self.get_ch_data(self.date_str,exchange_id,symbol)
                
                file_path = self.download_and_unzip(exchange_id,symbol,self.date_str)
                if file_path and os.path.exists(file_path):
                    official_df = self._changeColumns(exchange_id,symbol,file_path)
                    validator_trades(official_df)
                    
                    if gaps:
                        try:
                            all_patch_data = []

                            for row in gaps:
                                gap_size, prev_id, curr_id, _ = row

                                patch = official_df.filter(
                                    (pl.col('trade_id') > prev_id) & (pl.col('trade_id') < curr_id)
                                )

                                if not patch.is_empty():
                                    all_patch_data.append(patch)

                            if all_patch_data:
                                final_df = pl.concat(all_patch_data)
                                self.sync_to_clickhouse(exchange_id,symbol,final_df)
                                self.logger.info(f"✅ {exchange_id} {symbol} 补丁同步完成: 缝合 {len(final_df)} 条数据")

                        except Exception as e:
                            self.logger.error(f"❌ 处理 {exchange_id} {symbol} 补丁时出错: {e}")
                            

                    self.verify_full_integrity(exchange_id=exchange_id,symbol=symbol,official_df=official_df,file_path=file_path)

                # if os.path.exists(file_path):
                #     os.remove(file_path)

                if 'official_df' in locals(): del official_df
                if 'gaps' in locals(): del gaps

                gc.collect()

    def get_ch_data(self,target_date_str:str,exchange_id,symbol):
        dt = datetime.strptime(target_date_str,'%Y-%m-%d')
        check_start_date = (dt - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
        check_end_date = (dt + timedelta(days=1,minutes=1)).strftime('%Y-%m-%d %H:%M:%S')

        print(f"start_date: {check_start_date} | end_date: {check_end_date}")

        sql = f"""
            SELECT
                a.trade_id - a.prev_id - 1 AS gap_size,
                a.prev_id,
                a.trade_id,
                toDateTime(a.timestamp/1000) as gap_end_time
            FROM (
                SELECT 
                    toInt64(trade_id) AS trade_id,
                    timestamp,
                    lagInFrame(toInt64(trade_id)) OVER (PARTITION BY symbol ORDER BY toInt64(trade_id)) as prev_id
                FROM trades
                WHERE symbol='{symbol}' AND exchange_id='{exchange_id}'
                    AND timestamp >= toUnixTimestamp(toDateTime('{check_start_date}')) * 1000
                    AND timestamp <= toUnixTimestamp(toDateTime('{check_end_date}')) * 1000
            ) AS a
            WHERE a.prev_id > 0 
                AND (a.trade_id - a.prev_id) > 1
                AND toDateTime(a.timestamp/1000) >= toDateTime('{target_date_str}')
            ORDER BY a.trade_id ASC
        """

        result = self.ch_client.query(sql)
        if result.result_rows:
            for row in result.result_rows:
                self.logger.warning(f"🚨 发现空洞: {exchange_id} {symbol} | 范围: {row[1]} <-> {row[2]} | 缺失: {row[0]}条")
            return result.result_rows
        else:
            self.logger.info(f"✅ {exchange_id} {symbol} 数据在 ClickHouse 中完美连贯。")
            return []

    def download_and_unzip(self,exchange_id,symbol:str,date_obj):
        # symbol = symbol.replace('/','').replace('-','')
        # url = f"https://data.{exchange_id}.vision/data/spot/daily/trades/{symbol}/{symbol}-trades-{date_obj}.zip"
        # file_path = f"temp/{symbol}-trades-{date_obj}.csv"
        url,file_path = self._get_url(exchange_id,symbol,date_obj)
        os.makedirs(os.path.dirname(file_path),exist_ok=True)

        if os.path.exists(file_path):
            self.logger.info(f"file is exist {file_path}")
            return file_path
        
        self.logger.info(f"🌐 Rquesting a patch from binance")
        start_time = time.time()
      
        try:
            r = requests.get(url,timeout=20)
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                z.extractall(f"temp/{exchange_id}/")
                self.logger.info(f"📦 Download and zip confirmed. | Elapsed time: {time.time() - start_time:.2f}s | file path: {file_path}")
                return file_path
            else:
                self.logger.warning(f"Download failed,status code: {r.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Network error: Unable to connect to Binance API | Detail: {e}")
            raise ConnectionError(f"Binance API unavailable: {e}")
        
        except IOError as e:
            self.logger.error(f"❌ Disk failure: Unable to write data | Detail: {e}")
            raise

    def _get_url(self,exchange_id,symbol:str,date_obj:str):
        if exchange_id == 'binance':
            symbol = symbol.replace('/','').replace('-','')
            url = f"https://data.{exchange_id}.vision/data/spot/daily/trades/{symbol}/{symbol}-trades-{date_obj}.zip"
        elif exchange_id == 'okx':
            symbol = symbol.replace('/','-')
            clear_date = date_obj.replace('-','')
            url = f"https://static.okx.com/cdn/okex/traderecords/trades/daily/{clear_date}/{symbol}-trades-{date_obj}.zip"

        file_path = f"temp/{exchange_id}/{symbol}-trades-{date_obj}.csv"
        return url,file_path

    def _changeColumns(self,exchange_id,symbol,file_path:str):
        if exchange_id == 'binance':
            df = pl.scan_csv(file_path,has_header=False,new_columns=self.csv_columns[exchange_id]).collect()
            return df.with_columns([
                pl.lit(exchange_id).alias('exchange_id'),
                pl.lit(symbol).alias('symbol'),
                pl.col('trade_id').cast(pl.Int64),
                pl.col('price').cast(pl.Float64),
                pl.col('amount').cast(pl.Float64),
                (pl.col("timestamp") // 1000).cast(pl.Int64).alias("timestamp"),
                pl.when(pl.col('is_maker') == False).then(pl.lit('buy')).otherwise(pl.lit('sell')).alias('side'),
                pl.col('is_maker').not_().alias('is_taker_buyer'),
                pl.lit(int(time.time() * 1000)).alias('local_timestamp')
            ]).select(self.schema)
        elif exchange_id == 'okx':
            df = pl.scan_csv(file_path).collect()
            return df.with_columns([
                pl.lit(exchange_id).alias('exchange_id'),
                pl.lit(symbol).alias('symbol'),
                pl.col('trade_id').cast(pl.Int64),
                pl.col('price').cast(pl.Float64),
                pl.col('size').cast(pl.Float64).alias('amount'),
                pl.col("created_time").cast(pl.Int64).alias('timestamp'),
                pl.col('side'),
                (pl.col('side') == 'buy').alias('is_taker_buyer'),
                pl.lit(int(time.time() * 1000)).alias('local_timestamp')
            ]).select(self.schema)


    def sync_to_clickhouse(self,exchange_id,symbol,df:pl.DataFrame):
        formatted_df = df.with_columns(
            [pl.col('trade_id').cast(pl.String)]
        )
        # formatted_df = df.with_columns([
        #     pl.lit(self.exchange_id).alias('exchange_id'),
        #     pl.lit(symbol).alias('symbol'),
        #     pl.col('trade_id').cast(pl.String),
        #     pl.col('price').cast(pl.Float64),
        #     pl.col('amount').cast(pl.Float64),
        #     (pl.col("timestamp") // 1000).cast(pl.Int64).alias("timestamp"),
        #     pl.when(pl.col('is_maker') == False).then(pl.lit('buy')).otherwise(pl.lit('sell')).alias('side'),
        #     pl.col('is_maker').not_().alias('is_taker_buyer'),
        #     pl.lit(int(time.time() * 1000)).alias('local_timestamp')
        # ]).select(self.schema)
        self.logger.info(f"✅[{exchange_id}-{symbol}] 准备写入 {len(formatted_df)} 条数据!")
        try:

            data = formatted_df.rows()
            self.ch_client.insert(
                table='trades',
                data=data,
                column_names=self.schema
            )
            self.logger.info(f"✅[{exchange_id}-{symbol}] 成功缝合 {len(formatted_df)} 条数据!")

            del formatted_df
        except Exception as e:
            self.logger.error(f"🚨 ClickHouse 同步失败: {e}")
            raise

    def verify_full_integrity(self,exchange_id,symbol,official_df:pl.DataFrame,file_path):
        try:
            self.logger.info(f"🔍 开始全属性核对: {exchange_id} - {symbol} @ {self.date_str}")
            csv_df = official_df.with_columns(
                [pl.col('trade_id').cast(pl.String)]
            )
            # csv_df = official_df.with_columns([
            #     pl.col('trade_id').cast(pl.String),
            #     pl.col('price').cast(pl.Float64).round(8),
            #     pl.col('amount').cast(pl.Float64).round(8),
            #     (pl.col("timestamp") // 1000).cast(pl.Int64).alias("timestamp"),
            #     pl.when(pl.col('is_maker') == False).then(pl.lit('buy')).otherwise(pl.lit('sell')).alias('side'),
            #     pl.col('is_maker').not_().alias('is_taker_buyer')
            # ]).select(['trade_id','price','amount','timestamp','side','is_taker_buyer']).sort('trade_id')

            sql = f"""
                SELECT
                    trade_id,
                    round(price,8) as price,
                    round(amount,8) as amount,
                    timestamp,
                    side,
                    is_taker_buyer
                FROM
                    trades FINAL
                WHERE exchange_id='{exchange_id}' AND symbol='{symbol}'
                    AND timestamp >= toUnixTimestamp64Milli(toDateTime64('{self.date_str} 00:00:00',3,'UTC'))
                    AND timestamp < toUnixTimestamp64Milli(toDateTime64('{self.date_str} 00:00:00',3,'UTC') + INTERVAL 1 DAYS)
                ORDER BY trade_id ASC
            """
            ch_df = self.ch_client.query_df(sql)
            ch_df = pl.from_pandas(ch_df)

            if ch_df.is_empty():
                self.logger.error(f"❌ 核对失败：数据库中未找到 {exchange_id} {symbol} 的任何数据")
                return False

            diff = csv_df.join(ch_df,on='trade_id',how='anti')

            len_csv_df = len(csv_df)
            len_ch_df = len(ch_df)

            del ch_df
            del csv_df
            gc.collect()

            if diff.is_empty() and len_csv_df == len_ch_df:
                self.logger.info(f"✅ [全属性对账通过] {exchange_id} {symbol} 共 {len_ch_df} 条数据，每一位都与官方一致。")
                if os.path.exists(file_path):
                    os.remove(file_path)
                return True
            else:
                self.logger.error(f"🚨 [对账失败] {exchange_id} {symbol} 发现 {len(diff)} 条差异或缺失数据！")
                # 保存差异样本供排查
                diff_sample_path = f"temp/diff_{exchange_id}_{symbol.replace('/','')}_{self.date_str}.csv"
                diff.head(10).write_csv(diff_sample_path)
                self.logger.error(f"⚠️ 差异样本已保存至: {diff_sample_path}")
                return False


        except Exception as e:
            self.logger.error(f"🚨 全属性核对过程崩溃: {e}")
            return False

def patcher(target_date=None):
    """这是供 Airflow 调用的入口函数"""
    instance = DailyPatcher(target_date=target_date)
    instance.main()

if __name__ == '__main__':
    patcher()