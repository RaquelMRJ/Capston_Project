import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.stage_redshift import StageToRedshiftOperator
#from airflow.operators import (StageToRedshiftOperator)#, LoadFactOperator,
                                #LoadDimensionOperator, DataQualityOperator)
#from helpers import SqlQueries


default_args = {
    'owner': 'shravan',
    'start_date': datetime.utcnow() - timedelta(hours=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'provide_context': True,
}

dag = DAG(
        'copy_s3_to_redshift',
        default_args=default_args,
        description='Copy data from S3 to Redshift',
        schedule_interval=None,
        max_active_runs=1
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

copy_fact_movies_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_fact_movies_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "fact_movie_analytics",
    s3_bucket = "staging-raquel/fact_table",
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
    s3_bucket = "staging-raquel/dim_tables", #s3://staging-raquel/tables/fact_table/fact_movie_analytics.csv/
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
    s3_bucket = "staging-raquel/dim_tables",
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
    s3_bucket = "staging-raquel/dim_tables",
    s3_key = "dim_location",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2"
)

copy_dim_os_from_s3_to_redshift = StageToRedshiftOperator(
    task_id="copy_dim_os_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "dim_os",
    s3_bucket = "staging-raquel/dim_tables",
    s3_key = "dim_os",
    arn_iam_role = "arn:aws:iam::102817144359:role/service-role/AmazonRedshift-CommandsAccessRole-20220215T122325",
    region = "us-east-2",
    csv_format = "auto"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [copy_dim_date_from_s3_to_redshift,copy_dim_devices_from_s3_to_redshift, copy_dim_location_from_s3_to_redshift, copy_dim_os_from_s3_to_redshift, copy_fact_movies_from_s3_to_redshift] >> end_operator#, copy_dim_date_from_s3_to_redshift, copy_dim_devices_from_s3_to_redshift, copy_dim_location_from_s3_to_redshift, copy_dim_os_from_s3_to_redshift] >> end_operator