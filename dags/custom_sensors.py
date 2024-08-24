from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

class SparkClusterSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, spark_master_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark_master_url = spark_master_url

    def poke(self, context):
        try:
            spark = SparkSession.builder \
                .master(self.spark_master_url) \
                .appName("SparkClusterSensor") \
                .getOrCreate()
            spark.stop()
            return True
        except Exception as e:
            self.log.info(f"Spark cluster not ready: {str(e)}")
            return False
        


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from confluent_kafka.admin import AdminClient, NewTopic

class KafkaTopicCheckCreateOperator(BaseOperator):

    @apply_defaults
    def __init__(self, topic_name, kafka_conn_id='kafka_default', *args, **kwargs):
        super(KafkaTopicCheckCreateOperator, self).__init__(*args, **kwargs)
        self.topic_name = topic_name
        self.kafka_conn_id = kafka_conn_id

    def execute(self, context):
        # Create an AdminClient instance using the connection ID from Airflow
        kafka_admin = AdminClient({'bootstrap.servers': 'kafka-broker:29092'})  # replace with your Kafka server

        # Check if the topic exists
        topics = kafka_admin.list_topics().topics
        if self.topic_name in topics:
            self.log.info(f"Topic '{self.topic_name}' already exists.")
        else:
            self.log.info(f"Topic '{self.topic_name}' does not exist. Creating it.")
            new_topic = NewTopic(self.topic_name, num_partitions=1, replication_factor=1)
            kafka_admin.create_topics([new_topic])
            self.log.info(f"Topic '{self.topic_name}' created successfully.")

        return True






