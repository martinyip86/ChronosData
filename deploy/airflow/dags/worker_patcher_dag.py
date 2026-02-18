import sys
import os
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 【关键点 1】让 Airflow 容器能找到你的 src 模块
# 因为在 docker-compose 中我们将 ../../src 映射到了 /opt/airflow/src
sys.path.insert(0,'/opt/airflow')

# 现在可以安全地 import 你的业务逻辑了
# 假设你把原来的 main 改名为了 run_patch_logic
from src.workers.worker_patcher import main as run_patch_logic

default_args = {
    'owner': 'martin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay':timedelta(minutes=5),
}

with DAG(
    'binance_daily_patching',
    default_args=default_args,
    description='Auto remedy for Binance data gaps daily',
    schedule='@daily',
    start_date=datetime(2026,2,2),
    catchup=False,
    tags=['binance','data_quality']
) as dag:
    patching_task = PythonOperator(
        task_id='patch_btc_trades',
        python_callable=run_patch_logic,
        op_kwargs={
            "target_date":"{{ macros.ds_add(ds, -1) }}",
            "exchange":"Binance",
            "symbol":"BTCUSDT",
        }
    )

    patching_task