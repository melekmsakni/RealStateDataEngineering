from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_submit_dag',
    default_args=default_args,
    description='A DAG to submit a Spark job',
    schedule_interval=None,  # Set to None for manual triggering or adjust as needed
    start_date=days_ago(1),
    catchup=False,
)


spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    conn_id='spark_container',  # The connection ID you created for Spark
    application='/opt/airflow/jobs/spark-consumer.py',  # Path to your Spark job script 
    packages='com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1',
    dag=dag,
)


spark_submit_task

