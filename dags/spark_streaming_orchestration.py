from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from custom_sensors import SparkClusterSensor, KafkaTopicCheckCreateOperator
from datetime import datetime, timedelta


current_time = datetime.now()

# Calculate the start date (e.g., current date minus 1 day)
start_date = current_time - timedelta(days=1)


TOPIC='tayara_topic'


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": start_date,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}



dag = DAG(
    "cluster_sensor_and_spark_submit_example",
    default_args=default_args,
    description="Cluster Sensor and Spark Submit Example",
    schedule_interval="@once",
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)

spark_cluster_sensor = SparkClusterSensor(
    task_id="spark_cluster_sensor",
    spark_master_url="spark://spark-master:7077",  # Adjust this to your Spark master URL
    poke_interval=30,
    timeout=600,
    dag=dag,
)

check_or_create_topic = KafkaTopicCheckCreateOperator(
    task_id="check_or_create_kafka_topic",
    topic_name=TOPIC,
)




# SparkSubmitOperator to run after all sensors pass
spark_submit_task = SparkSubmitOperator(
    task_id="spark_submit_task",
    conn_id="spark_container",  # The connection ID you created for Spark
    application="/opt/airflow/jobs/spark-consumer.py",  # Path to your Spark job script
    packages="com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-avro_2.12:3.5.0",
    dag=dag,
)



start >> [spark_cluster_sensor, check_or_create_topic] >> spark_submit_task 
