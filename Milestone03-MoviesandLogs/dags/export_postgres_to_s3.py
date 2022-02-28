import airflow.utils.dates

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from custom_modules.dag_postgres_to_s3_operator import PostgresToS3Operator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG('dag_export_postgres_to_s3', default_args = default_args, schedule_interval = '@daily')

process_dag = PostgresToS3Operator(
    task_id = 'dag_postgres_to_s3',
    schema = 'dbpostgres',
    table= 'user_purchase',
    s3_bucket = 'staging-raquel',
    s3_key =  'user_purchase',
    query = "SELECT * FROM dbpostgres.user_purchase",
    aws_conn_postgres_id = 'postgres_default',
    aws_conn_id = 'aws_default',   
    dag = dag
)


process_dag