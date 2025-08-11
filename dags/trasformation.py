from datetime import datetime
from airflow.sdk import DAG, chain
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from scripts.notification import discord_notification

default_args = {
    "owner": "airflow",
    "depends_on_past": True
}

with DAG(
    dag_id="jcdeol005_capstone_3_transformation",
    default_args=default_args,
    description="Run DBT transformation via BashOperator",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["jcdeol005", "transformation", "dbt", "bigquery", "bash"]
) as dag:

    start = EmptyOperator(task_id="start")
    staging = BashOperator(
        task_id="create_dataset_staging",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select staging",
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification
    )

    model = BashOperator(
        task_id="create_dataset_model",
        bash_command=" dbt run --profiles-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select model",
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification
    )

    validasi = BashOperator(
        task_id="validasi_model",
        bash_command="dbt test --profiles-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select model",
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification
    )

    marts = BashOperator(
        task_id="create_dataset_marts",
        bash_command="dbt run --profiles-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select marts",
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification
    )

    end = EmptyOperator(task_id="end")
    chain(start, staging, model, validasi, marts, end)
