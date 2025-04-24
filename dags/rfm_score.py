from datetime import timedelta, datetime
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

local_tz = pendulum.timezone("Asia/Bangkok")
start_date = datetime(2025, 4, 24, tzinfo=local_tz)

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
    'simple_task_for_testing_mart_rfm_score',
    default_args=default_args,
    description='simple_task_for_testing',
    catchup=False,
    schedule_interval="30 0 * * *",  # Chạy DAG hàng ngày lúc 00:30
)

spark_submit_task = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/RFM_calculate.py',
    task_id='spark_submit_add_mart_rfm_score',
    conn_id='spark_default',
    verbose=True,
    name='spark_submit_add_mart_rfm_score',
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
