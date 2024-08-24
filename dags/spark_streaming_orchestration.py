from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from custom_sensors import SparkClusterSensor,KafkaTopicCheckCreateOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'cluster_sensor_and_spark_submit_example',
    default_args=default_args,
    description='Cluster Sensor and Spark Submit Example',
    schedule_interval="@daily",
    catchup=False,
)

start = EmptyOperator(task_id='start', dag=dag)

spark_cluster_sensor = SparkClusterSensor(
    task_id='spark_cluster_sensor',
    spark_master_url='spark://spark-master:7077',  # Adjust this to your Spark master URL
    poke_interval=30,
    timeout=600,
    dag=dag,
)

check_or_create_topic = KafkaTopicCheckCreateOperator(
    task_id='check_or_create_kafka_topic',
    topic_name='tecnocasa_topic',
)

# kafka_topic_sensor = AwaitMessageSensor(
#     kafka_config_id="kafka_default",
#     task_id="awaiting_message",
#     topics=["tecnocasa_topic"],
#     apply_function="custom_sensors.fun1",
#     xcom_push_key="retrieved_message",
#     dag=dag,
# )



# SparkSubmitOperator to run after all sensors pass
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    conn_id='spark_container',  # The connection ID you created for Spark
    application='/opt/airflow/jobs/spark-consumer.py',  # Path to your Spark job script 
    packages='com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1',
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

start >> [spark_cluster_sensor, check_or_create_topic] >> spark_submit_task >> end