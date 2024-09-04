from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
import os

# credentials = json.loads(os.popen("aws configure export-credentials").read())

default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "solution_local",
    default_args=default_args,
    schedule_interval=None,  # only trigerred manually
    catchup=False,
) as dag:
    clean = DockerOperator(
        task_id="clean_data",
        image="stackoverflowetl:v2",
        api_version="auto",
        auto_remove=True,
        command="python3 src/stackoverflowetl/tasks/clean.py --date {{ ds }} --env local",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_SESSION_TOKEN": os.environ["AWS_SESSION_TOKEN"],
        },
    )

    clean
