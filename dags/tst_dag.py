from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import logging
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

def tst():
    a = 3 + 4
    return a


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'on_failure_callback': send_failure_email,  # Thêm callback cho thất bại
    'on_success_callback': send_success_email,
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
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
