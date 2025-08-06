from datetime import datetime
from airflow.sdk import DAG, chain
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from docker.types import Mount
from scripts.notification import discord_notification
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": True
}

with DAG(
    dag_id="jcdeol005_capstone_3_transformation",
    default_args=default_args,
    description="Run DBT transformation in Docker container",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["jcdeol005", "transformation", "dbt", "bigquery", "docker"]
) as dag:

    start = EmptyOperator(task_id="start")

    staging = DockerOperator(
        task_id="create_dataset_staging",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.6.0",
        command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select staging",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success", 
        mount_tmp_dir=False,
        mounts=[
            Mount(source=os.path.abspath("./dbt"), target="/opt/airflow/dbt", type="bind"),
            Mount(source=os.path.abspath("./keys"), target="/opt/airflow/keys", type="bind"),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification,
    )

    model = DockerOperator(
        task_id="create_dataset_model",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.6.0",
        auto_remove="success",
        command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=os.path.abspath("./dbt"), target="/opt/airflow/dbt", type="bind"),
            Mount(source=os.path.abspath("./keys"), target="/opt/airflow/keys", type="bind"),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification,
    )

    validasi = DockerOperator(
        task_id="validasi_model",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.6.0",
        auto_remove="success",
        command="dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=os.path.abspath("./dbt"), target="/opt/airflow/dbt", type="bind"),
            Mount(source=os.path.abspath("./keys"), target="/opt/airflow/keys", type="bind"),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification,
    )

    marts = DockerOperator(
        task_id="create_dataset_marts",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.6.0",
        api_version="auto",
        auto_remove="success",
        command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --select marts",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(source=os.path.abspath("./dbt"), target="/opt/airflow/dbt", type="bind"),
            Mount(source=os.path.abspath("./keys"), target="/opt/airflow/keys", type="bind"),
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/keys/credentials.json"
        },
        on_failure_callback=discord_notification,
        on_success_callback=discord_notification,
    )

    end = EmptyOperator(task_id="end")

    chain(start, staging, model, validasi, marts, end)
