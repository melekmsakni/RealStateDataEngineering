from schema_registry.client import SchemaRegistryClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
import json
from fastavro.schema import load_schema


# Load the Avro schema from a .avsc file





avro_file = "/home/melek/RealStateDataEngineering/RealState_schema.avsc"
schema_registry_url = "http://localhost:8081"
schema_registry_subject = f"RealState-schema"


def avro_schema_to_string(file_path):
    schema = load_schema(file_path)
    schema_str = json.dumps(schema)
    return schema_str

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



SCHEMA_STR = avro_schema_to_string(avro_file)

schema_id = register_schema(schema_registry_url, schema_registry_subject, SCHEMA_STR)
print(schema_id)

sr, latest_version = get_schema_from_schema_registry(
    schema_registry_url, schema_registry_subject
)
print(latest_version.schema.schema_str)
