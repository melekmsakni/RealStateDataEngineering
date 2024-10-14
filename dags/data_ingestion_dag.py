from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from ingestion_scripts.tayara_ingestion import tayara_all_data


current_time = datetime.now()

# Calculate the start date (e.g., current date minus 1 day)
start_date = current_time - timedelta(days=1)

# Define default_args for your DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
with DAG(
    "data_ingestion_dag",
    start_date=start_date,
    schedule_interval="@once",
    description="A DAG for monitoring real-time data ingestion and processing",
    default_args=default_args,
    catchup=False,
) as dag:

    # Define tasks
    tecnocasa_ingestion = PythonOperator(
        task_id="tayara_ingestion",
        python_callable=tayara_all_data,
    )

    # send_email_notification = EmailOperator(
    #     task_id='send_email_notification',
    #     to='msaknimelek2@gmail.com',  # Replace with your email address
    #     subject='Data Ingestion and Processing Notification',
    #     html_content='<p>Data ingestion and processing are running successfully!</p>',
    #     dag=dag,
    # )

    # Define task dependencies
    tecnocasa_ingestion
