from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
import os

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "clean_dag",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    catchup=False,
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")

    end_dag = EmptyOperator(task_id="end_dag")

    task = DockerOperator(
        task_id="docker_clean",
        image="capstone_image",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_SESSION_TOKEN": os.environ.get("AWS_SESSION_TOKEN")
        }
    )

    start_dag >> task >> end_dag
