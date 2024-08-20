from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from ingestion_scripts.tecnocasa_ingestion import tecnocasa_all_data


# Define default_args for your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG("data_ingestion_dag", start_date=datetime(2024, 8, 14),
    schedule_interval="@daily",description='A DAG for monitoring real-time data ingestion and processing',default_args=default_args, catchup=False) as dag:


    # Define tasks
    tecnocasa_ingestion = PythonOperator(
        task_id='tecnocasa_ingestion',
        python_callable=tecnocasa_all_data,
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
