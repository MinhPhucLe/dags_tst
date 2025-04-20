from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging

start_date = datetime(2024, 12, 15)


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
    'simple_task_for_testing_migration',
    default_args=default_args,
    description='simple_task_for_testing',
    catchup=False,
    schedule=timedelta(days=1),  # Run the DAG daily
)

spark_submit_task = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/migrate.py',  # Path to the Java Spark application JAR
    task_id='spark_submit_migrate_python_task',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='migrate_dw_task',
    conf={
        'spark.submit.deployMode': 'cluster',
        'spark.master': 'yarn',
        'spark.hadoop.fs.defaultFS': 'hdfs://hadoop-hadoop-hdfs-nn:9000'
    },
    executor_cores=1,
    executor_memory='512m',
    driver_memory='512m',
    dag=dag
)
