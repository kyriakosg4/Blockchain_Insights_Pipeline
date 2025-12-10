from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_dbt_build",             # this is the DAG ID you will trigger
    start_date=datetime(2025, 12, 10),
    schedule_interval="10 0 * * *",       # runs daily at 00:10
    catchup=False,
    default_args=default_args,
    tags=["dbt"],
):

    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            "cd /opt/airflow/chainpulse_dbt && "
            "dbt build --profiles-dir ."
        )
    )
