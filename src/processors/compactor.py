import polars as pl
from datetime import datetime
from src.utils.logger import setup_logger
import glob
import os
import uuid
import shutil
import time

logger = setup_logger(name="Data Compactor")

class DataCompactor:
    def __init__(self,base_path="data/raw"):
        self.file_path = base_path
        self.buffer_days = 3

    def compact_day(self,date_obj:datetime,dtype:str):
        data_dir = os.path.join(self.file_path,date_obj.strftime("%Y/%m/%d"))
        files_path = os.path.join(
            data_dir,
            "[0-9]*.parquet"
        )
        files = sorted(glob.glob(files_path))

        if not files:
            logger.warning(f"Not exists files in files path: {files_path}")
            return

        unique_set = ['trade_id','timestamp'] if dtype == 'trade' else ['nonce','timestamp']

        df = pl.scan_parquet(files).unique(subset=unique_set).sort('timestamp').collect()

        final_file_name = f"{dtype}_{date_obj.strftime('%Y%m%d')}_final.parquet"
        final_path = os.path.join(data_dir,final_file_name)
        temp_path = f"{final_path}.{uuid.uuid4().hex}.tmp"
        try:
            df.write_parquet(temp_path,compression='snappy')
            os.replace(temp_path,final_path)
            logger.info(f"✅ 合拼成功: {final_path} ({len(df)} rows)")
        except Exception as e:
            raise e
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        
        if os.path.exists(final_path):
            self.move_archive(files,data_dir)

    def move_archive(self,files:list,data_dir:str):
        logger.info(f"开始移动缓冲区")
        archive_dir = os.path.join(data_dir,'.archive')
        os.makedirs(archive_dir,exist_ok=True)
        if len(files) > 0:
            for f in files:
                dest = os.path.join(archive_dir,os.path.basename(f))
                shutil.move(f,dest)

            logger.info(f"📦 碎文件已移至缓冲区: {archive_dir}")

    def clearup_archive(self,data_dir):
        """清理超过保留时间的归档文件"""
        archive_dir = os.path.join(data_dir,'.archive')
        if not os.path.exists(archive_dir):
            return

        now = time.time()

        retention_sec = self.buffer_days * 24 * 3600

        for f in os.listdir(archive_dir):
            file_path = os.path.join(archive_dir,f)

            if os.path.getmtime(file_path) < (now - retention_sec):
                try:
                    os.remove(file_path)
                    logger.info(f"🗑️ 彻底清理过期碎文件: {f}")
                except Exception as e:
                    logger.error(f"清理失败: {e}")