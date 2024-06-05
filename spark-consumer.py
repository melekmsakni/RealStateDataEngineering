import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, from_json


def insert_data(session, row):
    # Example function to insert data into Cassandra with the new schema
    query = """
    INSERT INTO property_streams.properties (
        ad_type, detail_url, surface, price, rooms, subtitle, title, images_list
    )
    VALUES (
        %(ad_type)s, %(detail_url)s, %(surface)s, %(price)s, %(rooms)s, %(subtitle)s, %(title)s, %(images_list)s
    )
    """
    session.execute(query, row)


def create_keyspace(session):
    session.execute(
        """
    CREATE KEYSPACE IF NOT EXISTS property_streams
    WITH replication = {'class': 'SimpleStrategy', replication_factor': '1'};
    """
    )
    print("key space created succeflly")


def create_table(session):
    session.execute(
        """
CREATE TABLE IF NOT EXISTS property_streams.properties (
    ad_type text,
    detail_url text,
    surface text,
    price text,
    rooms text,
    subtitle text,
    title text,
    images_list list<text>,
    PRIMARY KEY(detail_url)
);
"""
    )
    print("Table created successfully")


def cassandra_session():
    cluster = Cluster(["localhost"])
    session = cluster.connect()
    if session is not None:
        create_keyspace(session)
        create_table(session)

    return session


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
        .option("subscribe", "properties")
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType(
        [
            StructField("ad_type", StringType(), True),
            StructField("detail_url", StringType(), True),
            StructField("surface", StringType(), True),
            StructField("price", StringType(), True),
            StructField("rooms", StringType(), True),
            StructField("subtitle", StringType(), True),
            StructField("title", StringType(), True),
            StructField("images_list", ArrayType(StringType()), True),
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
            lambda row: insert_data(cassandra_session(), row.asDict())
        )
    ).start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
