from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.slack.operators.slack_api import SlackAPIPostOperator
from airflow.utils.dates import days_ago

# Define default_args for your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG("data_ingestion_dag", start_date=datetime(2024, 8, 14),
    schedule_interval="@daily",description='A DAG for monitoring real-time data ingestion and processing',default_args=default_args, catchup=False) as dag:



    # Define tasks
    tecnocasa_ingestion = PythonOperator(
        task_id='tecnocasa_ingestion',
        python_callable=dag,
    )

    # Task to consume data from Kafka (for monitoring or further processing)


    # Task to send a notification to Slack
    send_notification = SlackAPIPostOperator(
        task_id='send_slack_notification',
        token='your_slack_token',  # Set this securely in Airflow connections or secrets
        text='Data ingestion and processing are running successfully!',
        dag=dag,
    )

    # Define task dependencies
    tecnocasa_ingestion >>  send_notification 
