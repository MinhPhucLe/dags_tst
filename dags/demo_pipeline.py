from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

local_tz = pendulum.timezone("Asia/Bangkok")
start_date = datetime(2025, 4, 23, tzinfo=local_tz)
def send_email_via_smtp(subject, body, to_email):
    from_email = "tryrequestamin123@gmail.com"
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    smtp_user = "tryrequestamin123@gmail.com"
    smtp_password = "edaq udzq pqms jgyu"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(from_email, to_email, msg.as_string())

# Hàm gửi email khi thành công
def send_success_email(context):
    subject = f"DAG Success: {context['dag'].dag_id}"
    body = f"The DAG {context['dag'].dag_id} has successfully completed."
    send_email_via_smtp(subject, body, "tryrequestamin123@gmail.com")

# Hàm gửi email khi thất bại
def send_failure_email(context):
    subject = f"DAG Failed: {context['dag'].dag_id}"
    body = f"The DAG {context['dag'].dag_id} has failed."
    send_email_via_smtp(subject, body, "tryrequestamin123@gmail.com")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': send_failure_email,  # Thêm callback cho thất bại
    'on_success_callback': send_success_email,
    'retries': 5,
    'email_on_success': True,
    'email': ['tryrequestamin123@gmail.com'],
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'data_flow',
    default_args=default_args,
    description='Flow of data pipeline',
    catchup=False,
    schedule_interval="5 2 * * *",  # Run the DAG daily
)

spark_submit_task1 = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/mart_topviews.py',  # Path to the Java Spark application JAR
    task_id='ETL_Staging_to_Data_Warehouse',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='ETL_Staging_to_Data_Warehouse',
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

spark_submit_task2 = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/mart_topviews.py',  # Path to the Java Spark application JAR
    task_id='ETL_Staging_to_Data_Mart',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='ETL_Staging_to_Data_Mart',
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

spark_submit_task3 = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_spark/mart_topviews.py',  # Path to the Java Spark application JAR
    task_id='ETL_Data_Warehouse_to_Data_Mart',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='ETL_Data_Warehouse_to_Data_Mart',
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

spark_submit_task1 >> spark_submit_task2 >> spark_submit_task3

