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



def process_row(row, session):
    base_data = {
        "property_id": row["id"],
        "source": row["source"],
        "title": row["title"],
        "category": row["category"],
        "region": row["region"],
        "city": row["city"],
        "type": row["type"],
        "surface": row["surface"],
        "rooms": row["rooms"],
        "price": row["price"],
        "images": row["images"],
        "timestamp": row["timestamp"],
    }

    extended_data = {
        "property_id": row["id"],
        "source": row["source"],
        "source_specific_data": row["source_specific_data"],
    }

    insert_base_data(session, base_data)
    insert_extended_data(session, extended_data)


def cassandra_session():
    cluster = Cluster(["cassandra"])
    session = cluster.connect()
    # creating keyspace

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS real_estate WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
    )
    session.set_keyspace("real_estate")
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
        rooms INT,
        price DECIMAL,
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
        property_id, source, title, category, region,city, type, surface, rooms, price, images, timestamp
    )
    VALUES (
        %(property_id)s, %(source)s, %(title)s, %(category)s, %(region)s,%(city)s, %(type)s, %(surface)s, %(rooms)s, %(price)s, %(images)s, %(timestamp)s
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
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
            "org.apache.spark:spark-sql_2.13:3.5.0",
        )
        .getOrCreate()
    )
    # define the listener
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "tecnocasa_topic")
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
            StructField("rooms", IntegerType(), True),
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

    session = cassandra_session()
    # writing data to casansdra in microbashs
    query = kafka_df.writeStream.foreachBatch(
        lambda batch_df, batch_id: batch_df.foreach(
            lambda row: process_row(row, session)
        )
    ).start()
    query.awaitTermination()




if __name__ == "__main__":
    main()
