from datetime import datetime
from airflow.sdk import DAG , chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from scripts.notification import discord_notification
default_args = {
    "owner": "airflow",
    "depends_on_past": True
}
url = "https://data.cms.gov/sites/default/files/2023-04/402c0430-aab0-47f9-ab51-4274e30d2f79/beneficiary_2024.csv"
file = url.split('/')[-1]

with DAG(
    dag_id="jcdeol005_capstone_3_extraction",
    default_args=default_args,
    description="extract data to table staging bigquery",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["jcdeol005","extract","load_bigquery" "capstone-modul3"]
) as dag:
    start = EmptyOperator(task_id="start")

    download = BashOperator(
        task_id=f"download_beneficiary",
        bash_command="curl -X GET \"${base_url}\" -o /opt/airflow/tmp/${file}",
        env={
            "base_url": url,
            "file":file
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification 
        )
    create = BashOperator(
        task_id= "create_dataset",
        bash_command="python /opt/airflow/dags/scripts/create_raw.py",
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification 
    )

    load = BashOperator(
        task_id= "load_bigquery",
        bash_command="python /opt/airflow/dags/scripts/load_raw.py --year 2024",
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification 
    )
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command="rm -f /opt/airflow/tmp/*"
    )

    delay = BashOperator(
        task_id ='delay',
        bash_command = 'sleep 30'
    )

    chain(start, download, create, load, cleanup, delay)
