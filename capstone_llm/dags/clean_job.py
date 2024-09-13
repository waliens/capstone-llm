from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from conveyor.operators import ConveyorSparkSubmitOperatorV2
 
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
 
    task1 = ConveyorSparkSubmitOperatorV2(
        
        task_id="clean_data",
        num_executors=1,
        driver_instance_type="mx.small",
        executor_instance_type="mx.small",
        aws_role="capstone_conveyor_llm",
        application="/opt/spark/work-dir/src/capstonellm//tasks/clean.py" ,
        application_args=[
            "--env", "test"
        ]
       
    )
    start_dag >> task1 >> end_dag