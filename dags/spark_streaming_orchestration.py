from airflow import DAG
from airflow.operators.dummy import DummyOperator 
# from airflow.operators.custom_sensors import SparkClusterSensor, KafkaTopicSensor
from airflow.providers.apache.kafka.sensors.kafka_cluster import KafkaClusterSensor
from airflow.providers.apache.kafka.sensors.kafka import KafkaTopicSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 23),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('spark_streaming_orchestration',
          default_args=default_args,
          schedule_interval=None,
          catchup=False)

start = DummyOperator(task_id='start', dag=dag)

# Step 2: Spark Cluster Readiness Sensor
check_spark_cluster = SparkClusterSensor(
    task_id='check_spark_cluster',
    dag=dag,
    poke_interval=30,  # Check every 30 seconds
    timeout=600  # Timeout after 10 minutes
)

# Step 3: Kafka Topic Availability Sensor
check_kafka_topic = KafkaTopicSensor(
    task_id='check_kafka_topic',
    topic_name='your_topic_name',
    kafka_bootstrap_servers='your_kafka_broker:9092',
    poke_interval=30,
    timeout=600,
    dag=dag
)

# Step 4: Spark Submit Operator
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/path/to/your/spark/app.py',
    master='spark://spark-master:7077',
    conf={'spark.executor.memory': '2g'},
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Step 5: Task Dependencies
start >> check_spark_cluster >> check_kafka_topic >> spark_submit_task >> end
