from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging

local_tz = pendulum.timezone("Asia/Bangkok")
start_date = datetime(2024, 12, 21, tzinfo=local_tz)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'retries': 3,
    'email': ['tryrequestamin123@gmail.com'],
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'simple_task_for_testing_dim_date',
    default_args=default_args,
    description='simple_task_for_testing',
    catchup=False,
    schedule=timedelta(days=1),  # Run the DAG daily
)

spark_submit_task = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/tst_dim_time.py',  # Path to the Java Spark application JAR
    task_id='spark_submit_add_dim_time',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='spark_submit_add_dim_time',
    conf={
        'spark.submit.deployMode': 'cluster',
        'spark.master': 'yarn',
        'spark.hadoop.fs.defaultFS': 'hdfs://hadoop-hadoop-hdfs-nn:9000'
    },
    executor_cores=1,
    total_executor_cores=2,
    executor_memory='512m',
    driver_memory='512m',
    dag=dag
)
