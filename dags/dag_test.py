from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging

start_date = datetime(2024, 12, 15)


# def on_success_callback(context):
#     # Extract information from the context
#     task_instance = context['task_instance']
#     execution_date = context['execution_date']
#
#     # Format a message without the exception
#     message = f"Task {task_instance.task_id} was successful on {execution_date}"
#
#     logging.info(f"Sending success email: {message}")
#
#     # Send an email
#     try:
#         send_email(to='tryrequestamin123@gmail.com', subject='Task Success', html_content=message)
#         logging.info("Success email sent.")
#     except Exception as e:
#         logging.error(f"Failed to send success email: {e}")
#         raise

# def tst():
#     a = 3 + 4
#     return a


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 3,
    'email': ['tryrequestamin123@gmail.com'],
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'simple_task_for_testing',
    default_args=default_args,
    description='simple_task_for_testing',
    catchup=False,
    schedule=timedelta(days=1),  # Run the DAG daily
)

spark_submit_task = SparkSubmitOperator(
    application='hdfs://hadoop-hadoop-hdfs-nn:9000/test_task/spark_airflow-1.0-SNAPSHOT.jar',  # Path to the Java Spark application JAR
    java_class='org.example.simpleTask',  # Java class to execute
    task_id='spark_submit_java_task',
    conn_id='spark_default',  # Connection ID for Spark (preconfigured in Airflow)
    verbose=True,
    name='java_spark_task',
    conf={
        'spark.submit.deployMode': 'cluster',
        'spark.driver.extraClassPath': 'hdfs://hadoop-hadoop-hdfs-nn:9000/test_task/spark_airflow-1.0-SNAPSHOT.jar',
        'spark.executor.extraClassPath': 'hdfs://hadoop-hadoop-hdfs-nn:9000/test_task/spark_airflow-1.0-SNAPSHOT.jar',
    },
    executor_cores=1,
    executor_memory='512m',
    driver_memory='512m',
    dag=dag
)
