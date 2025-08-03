from datetime import datetime
from airflow.sdk import DAG , chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from scripts.notification import discord_notification
default_args = {
    "owner": "airflow",
    "depends_on_past": True
}

with DAG(
    dag_id="jcdeol005_capstone_3",
    default_args=default_args,
    description="extract data to table staging bigquery",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["jcdeol005", "capstone-modul3"]
) as dag:
    start = EmptyOperator(task_id="start")

    extract_to_staging = BashOperator(
        task_id= "load_staging",
        bash_command="python /opt/airflow/dags/scripts/load_staging.py",
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification 
    )

    delay = BashOperator(
        task_id ='delay',
        bash_command = 'sleep 30'
    )

    chain(start, extract_to_staging, delay)
