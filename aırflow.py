from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum
local_tz = pendulum.timezone("Europe/Istanbul")
file_path = '/Users/sdedeoglu/Desktop/python/raw_logs/test.py'

default_args = {
    'owner': 'sade',
    'email': None,
    'retries': 2,  # Case için retry ekledim
    'retry_delay': timedelta(minutes=5),  # Retry arası bekleme
    'email_on_failure': True,
    'email_on_retry': False
}

dag = DAG(
    dag_id='log_pipeline_daily',  # Case'e uygun isim
    default_args=default_args,
    description='Daily Log Pipeline - ETL for funnel data',  # Case açıklaması
    schedule_interval='@daily',  # Case gereksinimi: daily scheduling
    start_date=datetime(2024, 10, 19, tzinfo=local_tz),  # Güncel tarih
    catchup=False,  # Geçmiş tarihleri çalıştırma
    dagrun_timeout=timedelta(minutes=60),
    max_active_runs=1,  # Aynı anda sadece 1 çalışsın
    tags=['production', 'etl', 'log-pipeline']  # Case'e uygun taglar
)

# Basit ve çalışan task
run_pipeline = BashOperator(
    task_id='run_log_pipeline',
    bash_command=f'cd /Users/sdedeoglu/Desktop/python/raw_logs && python test.py',
    dag=dag
)

run_pipeline