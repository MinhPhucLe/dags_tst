from datetime import timedelta, datetime
from airflow import DAG
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

def tst():
    a = 3 + 4
    return a


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
    'snd_mail_test',
    default_args=default_args,
    description='snd_mail_test',
    catchup=False,
    schedule=timedelta(days=1),  # Run the DAG daily
)

task1 = PythonOperator(
    task_id='snd_mail_test',
    python_callable=tst,
    dag=dag
)
