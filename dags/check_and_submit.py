from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

local_tz = pendulum.timezone("Asia/Bangkok")
start_date = datetime(2025, 6, 23, tzinfo=local_tz)

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

def send_alert_email(context):
    subject = f"[ALERT] DAG: {context['dag'].dag_id}"
    body = "The streaming job consume data from kafka is stopping. Will submit again now"
    send_email_via_smtp(subject, body, "tryrequestamin123@gmail.com")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': True,
    'email_on_success': False,
    'email_on_retry': False,
    'on_failure_callback': send_alert_email,
    'email': ['tryrequestamin123@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# ============ Define DAG ============
with DAG(
    dag_id='monitor_spark_streaming_and_restart_if_crashed',
    default_args=default_args,
    description='Check if Spark Streaming job is running, restart if not',
    catchup=False,
    schedule_interval='*/5 * * * *',
) as dag:

    check_and_restart_job = BashOperator(
        task_id='check_and_submit_if_not_running',
        bash_command="""
        COUNT=$(curl -s "http://hadoop-hadoop-yarn-rm:8088/ws/v1/cluster/apps?applicationType=SPARK&states=RUNNING" \
            | grep -c "org.example.Example_consume")
        echo "Running job count: $COUNT"
        if [ "$COUNT" -eq 0 ]; then
            echo "Job not running. Submitting Spark Streaming job..."
            spark-submit \
              --master yarn \
              --deploy-mode cluster \
              --conf spark.executor.memory=512mb \
              --conf spark.driver.memory=512mb \
              --class org.example.Example_consume \
              --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.util=ALL-UNNAMED" \
              --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.util=ALL-UNNAMED" \
              --jars hdfs://hadoop-hadoop-hdfs-nn:9000/jars/streaming-job-1.0-SNAPSHOT.jar \
              hdfs://hadoop-hadoop-hdfs-nn:9000/jars/streaming-job-1.0-SNAPSHOT.jar
        else
            echo "Job already running. Nothing to do."
        fi
        """
    )
