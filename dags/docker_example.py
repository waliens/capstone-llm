from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "docker_operator_dag",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:
    start_dag = EmptyOperator(task_id="start_dag")

    end_dag = EmptyOperator(task_id="end_dag")

    task = DockerOperator(
        task_id="docker_command_sleep",
        image="capstone_image",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    start_dag >> task >> end_dag
