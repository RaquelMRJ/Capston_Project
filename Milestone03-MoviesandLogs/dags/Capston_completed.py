from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators import PythonOperator
from airflow.contrib.operators.emr_create_job_flow_operator import (EmrCreateJobFlowOperator)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (EmrTerminateJobFlowOperator)
import os
from airflow.operators import PostgresOperator
from helpers.create_tables import create_all_tables
import logging
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from operators.stage_redshift import StageToRedshiftOperator

################################### Transforming files #######################################
# Configurations
BUCKET_NAME1 = "raw-data-raquel"  
local_data = "./dags/data/movie_review.csv"
s3_data1 = "data/log_reviews.csv"
local_script = "./dags/scripts/spark/random_text_classification.py"
s3_script1 = "scripts/random_text_classification.py"
s3_clean1 = "s3://staging-raquel/clean_data/"
SPARK_STEPS1 = [

    {
        "Name": "Classify movie reviews",
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



default_args = {
    "owner": "Raquel",
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
    "Capston_DW",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)



# Add your steps to the EMR cluster
step_adder1 = EmrAddStepsOperator(
    task_id="Transforming_the_data",
    job_flow_id= "j-39BV6NLMK5QI0", #{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS1,
    params={
        "BUCKET_NAME": BUCKET_NAME1,
        "s3_data": s3_data1,
        "s3_script": s3_script1,
        "s3_clean": s3_clean1,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS1) - 1
# wait for the steps to complete
step_checker1 = EmrStepSensor(
    task_id="waiting_to_finish_transformation",
    job_flow_id="j-39BV6NLMK5QI0", #"{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Transforming_the_data', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)
############################################# creating data for tables ###########################################
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


# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="dim_data",
    job_flow_id= "j-39BV6NLMK5QI0", #{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
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
    job_flow_id= "j-39BV6NLMK5QI0", #{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
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
    task_id="waiting_to_create_data",
    job_flow_id="j-39BV6NLMK5QI0", #"{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='fact_data', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)



##################################### create tables ###############################

create_all_tables = PostgresOperator(
    task_id="create_all_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_all_tables
)
################################## load s3 data to redshift tables ########################

copy_fact_movies_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_fact_movies_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "fact_movie_analytics",
    s3_bucket = "staging-raquel/tables/fact_table",   
    s3_key = "fact_movie_analytics.csv",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2"
    #timeformat AS 'auto' #or dateformat AS 'MM/DD/YYYY'
    #csv_format = "s3://staging-raquel/tables/fact_table/fact_movie_analytics.csv/"
    #csv_format = FORMAT AS { AVRO | JSON } 's3://jsonpaths_file';#"fact_movie_analytics.csv"
)

copy_dim_date_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_dim_date_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "dim_date",
    s3_bucket = "staging-raquel/tables/dim_tables", #s3://staging-raquel/tables/fact_table/fact_movie_analytics.csv/
    s3_key = "dim_date",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2"
)

copy_dim_devices_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_dim_devices_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "dim_devices",
    s3_bucket = "staging-raquel/tables/dim_tables",
    s3_key = "dim_devices",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2"
)

copy_dim_location_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_dim_location_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "dim_location",
    s3_bucket = "staging-raquel/tables/dim_tables",
    s3_key = "dim_location",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2"
)

copy_dim_browser_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_dim_browser_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "dim_browser",
    s3_bucket = "staging-raquel/tables/dim_tables",
    s3_key = "dim_browser",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2"
)
copy_dim_os_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_dim_os_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "dim_os",
    s3_bucket = "staging-raquel/tables/dim_tables",
    s3_key = "dim_os",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2",
    csv_format = "auto"
)

################################### end process ###############################################

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline  >> step_adder1 >> step_checker1 >> step_adder >> step_adder2 >> step_checker >> create_all_tables 
create_all_tables >> [copy_dim_date_from_s3_to_redshift,copy_dim_devices_from_s3_to_redshift, 
copy_dim_location_from_s3_to_redshift, copy_dim_browser_from_s3_to_redshift, copy_dim_os_from_s3_to_redshift, 
copy_fact_movies_from_s3_to_redshift] >> end_data_pipeline
