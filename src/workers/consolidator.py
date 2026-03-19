from src.storage.ch_client import ch_manager
from src.utils.logger import setup_logger
import os
import gc
from datetime import datetime,timedelta,timezone


class Consolidator:
    def __init__(self,target_date:str=None):
        self.ch_client = None
        self.logger = setup_logger("workers.consolidator")
        self.target_date = target_date
        self.exchanges = ['binance','okx']
        self.symbols = ['BTC/USDT','ETH/USDT','SOL/USDT','PEPE/USDT']
        self.data_types = ['orderbook','trades']
        self.mkt_types = ['spot']
        self.fields = {
            'orderbook':"""
                    nonce,
                    symbol,
                    mkt_type,
                    exchange_id,
                    fromUnixTimestamp64Milli(timestamp,'UTC') AS dt,
                    (bid_prices[1] * ask_volumes[1] + ask_prices[1] * bid_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS micro_price,
                    (bid_volumes[1] - ask_volumes[1]) / nullIf(bid_volumes[1] + ask_volumes[1],0) AS imbalance,
                    ask_prices[1] - bid_prices[1] AS spread,
                    arraySlice(bid_prices,1,20) AS bid_prices,
                    arraySlice(bid_volumes,1,20) AS bid_volumes,
                    arraySlice(ask_prices,1,20) AS ask_prices,
                    arraySlice(ask_volumes,1,20) AS ask_volumes,
                    timestamp""",
            'trades':"""
                trade_id,
                symbol,
                mkt_type,
                exchange_id,
                fromUnixTimestamp64Milli(timestamp,'UTC') AS dt,
                price,
                amount,
                price * amount AS turnover,
                side,
                is_taker_buyer,
                row_number() OVER (ORDER BY trade_id) AS sub_ms_seq,
                avg(price) OVER (ORDER BY trade_id ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) as ma_price_100,
                if(price * amount > 50000, 1, 0) as is_high_impact,
                timestamp
            """
        }
        self.sort_keys = {
            'orderbook': 'nonce',
            'trades': 'trade_id'
        }

    def setup(self):
        self.ch_client = ch_manager.market_db

    def daily_feature_consolidation(self,symbol:str,exchange_id:str,mkt_type:str,data_type:str,current_date:str):
        clear_symbol = symbol.replace('/','-')
        table_name = f"market_data.{data_type}"
        target_date_obj = datetime.strptime(current_date,'%Y-%m-%d')
        dir_path = os.path.join(
            "data/processed",
            exchange_id,
            mkt_type,
            clear_symbol,
            data_type
        )
        os.makedirs(dir_path,exist_ok=True)
        file_path = os.path.join(
            dir_path,
            f"{target_date_obj.strftime('%Y%m%d')}.parquet"
        )
        if os.path.exists(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if size_mb < 1:
                os.remove(file_path)

        if not os.path.exists(file_path):
            start_ms = int(target_date_obj.replace(tzinfo=timezone.utc).timestamp() * 1000)
            end_ms = start_ms + (24 * 60 * 60 * 1000) - 1
            sql = f"""
                INSERT INTO FUNCTION file('{file_path}','Parquet')
                SELECT
                    {self.fields[data_type]}
                FROM {table_name} FINAL
                WHERE symbol='{symbol}' 
                    AND exchange_id='{exchange_id}' 
                    AND mkt_type='{mkt_type}'
                    AND timestamp >= {start_ms}
                    AND timestamp <= {end_ms}
                ORDER BY {self.sort_keys[data_type]} ASC
            """
            try:
                self.logger.info(f"📊 开始执行日度特征物化: {exchange_id} {symbol} @ {current_date}")
                self.ch_client.command(sql)
                size_mb = os.path.getsize(file_path) / (1024 * 1024)
                self.logger.info(f"✨ 导出成功: {file_path} | size: {size_mb:.2f}MB")
            except Exception as e:
                self.logger.error(f"导出失败: {e}")
                raise

        gc.collect()

    def run(self):
        self.setup()

        is_automated = self.target_date is None

        for exchange_id in self.exchanges:
            if is_automated:
                days_offset = 2 if exchange_id == 'okx' else 1
                current_date = (datetime.now(timezone.utc) - timedelta(days=days_offset)).strftime('%Y-%m-%d')
            else:
                current_date = self.target_date

            for mkt_type in self.mkt_types:
                for symbol in self.symbols:
                    for data_type in self.data_types:
                        self.daily_feature_consolidation(
                            symbol=symbol,
                            exchange_id=exchange_id,
                            mkt_type=mkt_type,
                            data_type=data_type,
                            current_date=current_date
                        )

def consolidator(target_date: str=None):
    consolidator_obj = Consolidator(target_date)
    consolidator_obj.run()

if __name__ == '__main__':
    consolidator()
                            