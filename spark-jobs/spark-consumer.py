import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
    DecimalType,
    MapType,
)
from pyspark.sql.functions import col, from_json
from datetime import datetime
from cassandra.policies import DCAwareRoundRobinPolicy


testing='aut'
kafka_topic=f'tecnocasa_topic_{testing}'
keyspace_name=f'real_estate_{testing}'

def insert_cassandra(row):
    session = cassandra_session() 

    source_specific_data = row["source_specific_data"] if row["source_specific_data"] is not None else {}
    images = row["images"] if row["images"] is not None else []

    id= row["id"] if row["id"] is not  None else "gggg"
        

    # Convert None values to "null" string to avoid cassandra null problem in list/map
    for key, value in source_specific_data.items():
        if value is None:
            source_specific_data[key] = "null"

    base_data = {
        "property_id": id,
        "link":row["link"],
        "source": row["source"],
        "title": row["title"],
        "category": row["category"],
        "region": row["region"],
        "city": row["city"],
        "type": row["type"],
        "surface": row["surface"],
        "rooms": row["rooms"],
        "price": row["price"],
        "images": images,
        "timestamp": row["timestamp"],
    }

    extended_data = {
        "property_id": id,
        "source": row["source"],
        "source_specific_data": source_specific_data,
    }

    insert_base_data(session, base_data)
    insert_extended_data(session, extended_data)
    logging.info("row inserted succefully")


def cassandra_session():
    cluster = Cluster(["cassandra"],protocol_version=5,load_balancing_policy=DCAwareRoundRobinPolicy())
    session = cluster.connect()
    # creating keyspace
    
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};"
    )
    session.set_keyspace(keyspace_name)
    # creating tables

    session.execute(
        """
    CREATE TABLE IF NOT EXISTS real_estate_base (
        property_id TEXT PRIMARY KEY,
        source TEXT,
        title TEXT,
        category TEXT,
        region TEXT,
        city TEXT,
        type TEXT,
        surface DECIMAL,
        rooms DECIMAL,
        price DECIMAL,
        link TEXT,
        images LIST<TEXT>,
        timestamp TIMESTAMP
    );
    """
    )

    session.execute(
        """
    CREATE TABLE IF NOT EXISTS real_estate_extended (
        property_id TEXT PRIMARY KEY,
        source TEXT,
        source_specific_data MAP<TEXT, TEXT>
    );
    """
    )
    return session


def insert_base_data(session, row):
    query = """
    INSERT INTO real_estate_base (
        property_id, source, title, category, region,city, type, surface, rooms, price, images, timestamp,link
    )
    VALUES (
        %(property_id)s, %(source)s, %(title)s, %(category)s, %(region)s,%(city)s, %(type)s, %(surface)s, %(rooms)s, %(price)s, %(images)s, %(timestamp)s,%(link)s
    )
    """
    session.execute(query, row)


def insert_extended_data(session, row):
    query = """
    INSERT INTO real_estate_extended (
        property_id,source, source_specific_data
    )
    VALUES (
        %(property_id)s,%(source)s, %(source_specific_data)s
    )
    """
    session.execute(query, row)


def main():
    logging.basicConfig(level=logging.INFO)
    # lunching spark session with connectors
    spark = (
        SparkSession.builder.appName("realStateConsumer")
        .config("spark.cassandra.connection.host", "localhost")
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        )
        .getOrCreate()
    )




    # define the listener
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-broker:29092")
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("title", StringType(), True),
            StructField("category", StringType(), True),
            StructField("region", StringType(), True),
            StructField("city", StringType(), True),
            StructField("source", StringType(), True),
            StructField("link", StringType(), True),
            StructField("type", StringType(), True),
            StructField("surface", DecimalType(), True),
            StructField("rooms", DecimalType(), True),
            StructField("price", DecimalType(), True),
            StructField("images", ArrayType(StringType()), True),
            StructField("timestamp", StringType(), True),
            StructField(
                "source_specific_data", MapType(StringType(), StringType()), True
            ),
        ]
    )
    # getting data from kafka

    kafka_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as value")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    
    # writing data to casansdra in microbashs
    query = kafka_df.writeStream.foreachBatch(
        lambda batch_df, batch_id: batch_df.foreach(
            lambda row: insert_cassandra(row)
        )
    ).start()
    query.awaitTermination()




if __name__ == "__main__":
    main()
