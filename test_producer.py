import requests
import json

# 3rd party library imported
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import Schema

# imort from constants


data = {"id": None}
kafka_url = "localhost:9092"
schema_registry_url = "http://localhost:8081"
kafka_topic = "njarbAVRO"
schema_registry_subject = "RealState-schema"


def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print(
        "Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


def avro_producer(data, kafka_url, schema_registry_url, schema_registry_subject):

    try:
        sr, latest_version = get_schema_from_schema_registry(
            schema_registry_url, schema_registry_subject
        )
    except Exception as e:
        print(f"Error get_schema_from_schema_registry: {e}")

    #  it sets up the serializer, but does not yet perform the serialization.
    try:
        value_avro_serializer = AvroSerializer(
            schema_registry_client=sr,
            schema_str=latest_version.schema.schema_str,
        )

    except Exception as e:
        print(f"Error AvroSerializer: {e}")

    try:
        producer = SerializingProducer(
            {
                "bootstrap.servers": kafka_url,
                "security.protocol": "plaintext",
                "value.serializer": value_avro_serializer,
                "delivery.timeout.ms": 120000,  # set it to 2 mins
                "enable.idempotence": "true",
            }
        )
    except Exception as e:
        print(f"Error SerializingProducer: {e}")

    try:

        producer.produce(topic=kafka_topic, value=data, on_delivery=delivery_report)

        # Trigger any available delivery report callbacks from previous produce() calls
        events_processed = producer.poll(1)
        print(f"events_processed: {events_processed}")

        # Ensure messages are being sent in batches and check queue status
        messages_in_queue = producer.flush(1)
        print(f"messages_in_queue: {messages_in_queue}")

    except Exception as e:
        print(f"Error producing message: {e}")


def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({"url": schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version


def register_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({"url": schema_registry_url})
    schema = Schema(schema_str, schema_type="AVRO")
    schema_id = sr.register_schema(subject_name=schema_registry_subject, schema=schema)

    return schema_id


def update_schema(schema_registry_url, schema_registry_subject, schema_str):
    sr = SchemaRegistryClient({"url": schema_registry_url})
    versions_deleted_list = sr.delete_subject(schema_registry_subject)
    print(f"versions of schema deleted list: {versions_deleted_list}")

    schema_id = register_schema(
        schema_registry_url, schema_registry_subject, schema_str
    )
    return schema_id


avro_producer(data, kafka_url, schema_registry_url, schema_registry_subject)
