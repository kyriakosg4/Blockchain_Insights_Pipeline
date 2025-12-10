from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from pipelines.pipeline_live import fetch_latest_binance

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="live_binance_ingestion",
    start_date=datetime(2025, 12, 10),
    schedule_interval="0 * * * *",  # every hour
    catchup=False,
    default_args=default_args,
):

    ingest_prices = PythonOperator(
        task_id="fetch_binance_live",
        python_callable=fetch_latest_binance,
    )
