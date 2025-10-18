# --------------------------- Airflow DAG (MySQL-only) ---------------------------
# Save this block to airflow/dags/funnel_pipeline_dag.py in your Airflow environment.


from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os


DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='funnel_pipeline_mysql',
    default_args=DEFAULT_ARGS,
    description='Daily ETL that loads funnel data into MySQL using staging+upsert',
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl','funnel']
) as dag:

    def run_pipeline_for_date(**context):
        # The pipeline can be parametrized to process a date / path; here we assume the data path follows a pattern
        proc_date = context.get('ds')  # yyyy-mm-dd
        # Build data path based on date pattern, or use GCS connector to list
        data_path = f"/data/funnel/{{proc_date}}/events.parquet".format(proc_date=proc_date)
        cfg_path = "/opt/airflow/configs/db_config.json"
        p = FunnelPipeline(data_path=data_path, config_path=cfg_path)
        p.run()

    run_etl = PythonOperator(
        task_id='run_funnel_pipeline',
        python_callable=run_pipeline_for_date,
        provide_context=True,
    )

    def dq_only_check(**context):
        # Optional: run DQ queries against MySQL or run small checks on sample
        pass

    run_etl


# For convenience, write DAG content to a local file so you can copy to Airflow.
try:
    dag_out = Path('funnel_pipeline_dag.py')
    dag_out.write_text(airflow_dag_content, encoding='utf-8')
    logger.info('Airflow DAG file written to funnel_pipeline_dag.py (copy to your Airflow dags directory).')
except Exception:
    logger.exception('Failed to write DAG file locally; please copy the dag content from the variable.')

# End of file
