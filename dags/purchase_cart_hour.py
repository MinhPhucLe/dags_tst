from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging

local_tz = pendulum.timezone("Asia/Bangkok")
start_date = datetime(2025, 4, 25, tzinfo=local_tz)

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
    'simple_task_for_purchase_cart_hour',
    default_args=default_args,
    description='simple_task_for_purchase_cart_hour',
    catchup=False,
    schedule_interval="10 * * * *",  # Run the DAG daily
)

spark_submit_task = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/mart_action.py',  # Path to the Java Spark application JAR
    task_id='spark_submit_purchase_cart_hour',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='spark_submit_purchase_cart_hour',
    conf={
        'spark.submit.deployMode': 'cluster',
        'spark.master': 'yarn',
        'spark.hadoop.fs.defaultFS': 'hdfs://hadoop-hadoop-hdfs-nn:9000',
        'spark.executor.memory': '512m',
        'spark.executor.cores': '1',
        'spark.driver.memory': '512m',
        'spark.executor.instances': '1'
    },
    executor_cores=1,
    total_executor_cores=2,
    executor_memory='512m',
    driver_memory='512m',
    dag=dag
)
