from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipelines.pipeline_gap_repair import repair_yesterday

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_gap_repair",
    start_date=datetime(2025, 12, 10),
    schedule_interval="5 0 * * *",  # run at 00:05 daily
    catchup=False,
    default_args=default_args,
):

    repair_task = PythonOperator(
        task_id="repair_yesterday",
        python_callable=repair_yesterday,
    )
