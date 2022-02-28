from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)

# Configurations
BUCKET_NAME = "raw-data-raquel"
s3_data = "staging-raquel/"
s3_script = "scripts/dim_tables.py"
s3_script2 = "scripts/fact_table.py"
s3_clean = "s3://staging-raquel/tables/"
SPARK_STEPS = [

    {
        "Name": "Creating tables",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script }}",
            ],
        },
    }
]

SPARK_STEPS2 = [

    {
        "Name": "Creating tables",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.BUCKET_NAME }}/{{ params.s3_script2 }}",
            ],
        },
    }
]


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Create_dim_and_fact_tables_data",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)



# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="dim_data",
    job_flow_id= "j-YRMGIN2WOLX3", #{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script": s3_script,
        "s3_clean": s3_clean,
    },
    dag=dag,
)

step_adder2 = EmrAddStepsOperator(
    task_id="fact_data",
    job_flow_id= "j-YRMGIN2WOLX3", #{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS2,
    params={
        "BUCKET_NAME": BUCKET_NAME,
        "s3_data": s3_data,
        "s3_script2": s3_script2,
        "s3_clean": s3_clean,
    },
    dag=dag,
)


last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="j-YRMGIN2WOLX3", #"{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='fact_data', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)


end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline  >> step_adder >> step_adder2 >> step_checker >> end_data_pipeline
