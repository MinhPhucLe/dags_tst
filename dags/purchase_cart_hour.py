from datetime import timedelta, datetime

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging

local_tz = pendulum.timezone("Asia/Bangkok")
start_date = datetime(2025, 4, 25, tzinfo=local_tz)

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

start_date = datetime(2024, 12, 15)

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
    'email_on_retry': True,
    'email_on_success': True,
    'on_failure_callback': send_failure_email,  # Thêm callback cho thất bại
    'on_success_callback': send_success_email,
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
