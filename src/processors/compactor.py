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

        final_file_name = f"{dtype}_{date_obj.strftime('%Y%m%d')}_final.parquet"
        final_path = os.path.join(data_dir,final_file_name)
        temp_path = f"{final_path}.{uuid.uuid4().hex}.tmp"
        df = pl.scan_parquet(files).collect(engine='streaming')
        mem_mb = df.estimated_size() / (1024**2)
        logger.info(f"该文件在内存中占用: {mem_mb:.2f} MB")
        try:
            # pl.scan_parquet(files).unique(subset=unique_set).sort('timestamp').sink_parquet(
            #     temp_path,
            #     compression='snappy',
            #     row_group_size=100_000
            # )
            df.write_parquet(temp_path,compression='snappy')
            os.replace(temp_path,final_path)
            logger.info(f"✅ 合拼成功: {final_path},total rows: {len(df)}")
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

def compact_day_split_version(self,date_obj:datetime,dtype:str):
    data_dir = os.path.join(self.file_path,date_obj.strftime("%Y/%m/%d"))
    unique_set = ['trade_id','timestamp'] if dtype == 'trade' else ['nonce','timestamp']
    for h in range(25):
        hour_str = 'backfill' if h == 24 else f"{h:02d}"
        files_path = os.path.join(
            data_dir,
            f"{date_obj.strftime('%Y%m%d')}_{hour_str}*.parquet"
        )
        files = sorted(glob.glob(files_path))

        if not files:
            logger.debug(f"跳过小时 {hour_str}: 无文件")
            continue

        split_file_name = f"{dtype}_{date_obj.strftime('%Y%m%d')}_{hour_str}_split.parquet"
        split_path = os.path.join(data_dir,split_file_name)
        temp_path = f"{split_path}.{uuid.uuid4().hex}.tmp"
        try:
            os.environ["POLARS_MAX_THREADS"] = "2"

            df = pl.scan_parquet(files).unique(subset=unique_set).sort('timestamp').collect(engine='streaming')
            df.write_parquet(temp_path,compression='snappy')   
            os.replace(temp_path,split_path)
            logger.info(f"✅[hour: {hour_str}] combine successed: {split_path},rows: {len(df)}")

            del df

            gc.collect()

            if os.path.exists(split_path):
                self.move_archive(files,data_dir)
        except Exception as e:
            logger.error(f"❌ 处理 {hour_str} 失败: {e}")
            raise e
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
    
    gc.collect()
    final_files_path = os.path.join(
        data_dir,
        "*_split.parquet"
    )
    final_files = sorted(glob.glob(final_files_path))
    if not final_files:
        logger.warning(f"Not exists files in files path: {final_files_path}")
        return
    try:
        os.environ["POLARS_MAX_THREADS"] = "1"
        final_file_name = f"{dtype}_{date_obj.strftime('%Y%m%d')}_final.parquet"
        final_path = os.path.join(data_dir,final_file_name)
        final_temp_path = f"{final_path}.{uuid.uuid4().hex}.tmp"
        pl.scan_parquet(final_files).unique(subset=unique_set).sort('timestamp').sink_parquet(final_temp_path)
        os.replace(final_temp_path,final_path)
        logger.info(f"🚀 全天数据最终合并完成: {final_path}")
    except Exception as e:
        raise e
    finally:
        if os.path.exists(final_temp_path):
            os.remove(final_temp_path)

    if os.path.exists(final_path):
        for f in final_files:
            os.remove(f)