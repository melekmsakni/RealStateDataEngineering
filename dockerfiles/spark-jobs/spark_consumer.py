# pyspark imports
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import TimestampType
from cassandra.cluster import Cluster
# schema registry imports
from confluent_kafka.schema_registry import SchemaRegistryClient
from cassandra.policies import DCAwareRoundRobinPolicy
import json
import logging
from pyspark.sql.functions import datediff, current_date


kafka_url = "kafka-broker:29092"
schema_registry_url = "http://schema-registry:8081"
kafka_producer_topic = "tayara_topic"
schema_registry_subject = f"RealState-schema"

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version


def cassandra_session():
    cluster = Cluster(["cassandra"],protocol_version=5,load_balancing_policy=DCAwareRoundRobinPolicy())
    session = cluster.connect()
    # creating keyspace
    
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS real_estate_data_one WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};"
    )
    session.set_keyspace('real_estate_data_one')
    # creating tables

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS real_estate_data_one (
            id TEXT PRIMARY KEY,
            category TEXT,
            delegation TEXT,
            description TEXT,
            governorate TEXT,
            images LIST<TEXT>,
            isShop BOOLEAN,
            price DECIMAL,
            publishedOn TEXT,
            publisher TEXT,
            rooms INT,
            surface DECIMAL,
            title TEXT,
            type TEXT,
            bathrooms INT,
            price_per_square_meter DECIMAL,
            listing_age_days INT,
            num_images INT
        );
        """
    )

    return session


def insert_base_data(session, row):
    query = """
        INSERT INTO real_estate_data_one (
            id, category, delegation, description, governorate, images, isShop, price, publishedOn, publisher, rooms, surface, title, type, bathrooms,
            price_per_square_meter, listing_age_days, num_images  -- New Columns Added
        )
        VALUES (
            %(id)s, %(category)s, %(delegation)s, %(description)s, %(governorate)s, %(images)s, %(isShop)s, %(price)s, %(publishedOn)s, %(publisher)s, %(rooms)s, %(surface)s, %(title)s, %(type)s, %(bathrooms)s,
            %(price_per_square_meter)s, %(listing_age_days)s, %(num_images)s  -- New Columns Added
        )
    """


    session.execute(query, row)



def insert_cassandra(row):
    session = cassandra_session()

    if row["id"]==None :
        logging.info('null id')
        return  

    data = {
        "bathrooms": row["bathrooms"],  # 'int' or None
        "category": row["category"],    # 'string' or None
        "delegation": row["delegation"],  # 'string' or None
        "description": row["description"],  # 'string' or None
        "governorate": row["governorate"],  # 'string' or None
        "id": row["id"],  # 'string' or None
        "images": row["images"] if row["images"] else [],  # 'array' of strings or empty list
        "isShop": row["isShop"],  # 'boolean' or None
        "price": row["price"],  # 'double' or None
        "publishedOn": row["publishedOn"],  # 'string' or None
        "publisher": row["publisher"],  # 'string' or None
        "rooms": row["rooms"],  # 'int' or None
        "surface": row["surface"],  # 'double' or None
        "title": row["title"],  # 'string' or None
        "type": row["type"],  # 'string' or None
        "price_per_square_meter": row['price_per_square_meter'],  # New field
        "listing_age_days": row['listing_age_days'],  # New field
        "num_images": row['num_images']  # New field
    }




    insert_base_data(session, data)

    logging.info("row inserted succefully")


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("data_consumer").config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "org.apache.spark:spark-avro_2.12:3.5.0,"
            "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
        ).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

if __name__ == "__main__":
    real_state_df = spark \
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_url)\
    .option("subscribe", kafka_producer_topic)\
    .option("startingOffsets", "earliest")\
    .load()

    # real_state_df.printSchema()

    # note : based on the dafaumt DF we got from the streaming we prepeared data before serlization 
    tayara_df = real_state_df.withColumn("magicByte", func.expr("substring(value, 1, 1)"))
    tayara_df = tayara_df.withColumn("valueSchemaId", func.expr("substring(value, 2, 4)"))
    tayara_df = tayara_df.withColumn("fixedValue", func.expr("substring(value, 6, length(value)-5)"))
    tayara_value_df = tayara_df.select("magicByte", "valueSchemaId", "fixedValue")

    # tayara_value_df \
    # .writeStream \
    # .format("console") \
    # .outputMode("append") \
    # .option("truncate", "true") \
    # .start() \
    # .awaitTermination()


    _, latest_version_subject = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)
    
    
    

    # deserialize data 
    fromAvroOptions = {"mode":"PERMISSIVE"}   #any corruped error will be ignored
      
    decoded_output = tayara_df.select(
        from_avro(
            func.col("fixedValue"), latest_version_subject.schema.schema_str, fromAvroOptions
        )
        .alias("tayara")
    )

    

    tayara_value_df = decoded_output.select("tayara.*")


    tayara_value_df = tayara_value_df.withColumn(
    "price_per_square_meter", 
    func.when(func.col("surface") > 0, func.col("price") / func.col("surface"))
        .otherwise(None)
)

    
    tayara_value_df = tayara_value_df.withColumn(
        "listing_age_days", 
        datediff(current_date(), func.to_date(func.col("publishedOn"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
)
    tayara_value_df = tayara_value_df.withColumn(
        "num_images", 
        func.size(func.col("images"))
)

    

    tayara_value_df.printSchema()

    query = tayara_value_df.writeStream.foreachBatch(
        lambda batch_df, batch_id: batch_df.foreach(
            lambda row: insert_cassandra(row)
        )
    ).start()
    query.awaitTermination()
    exit()

#########################################################################################################


    tayara_value_df = tayara_value_df \
        .filter(func.col("isShop") == True) \
        .select("type","publishedOn", "publisher","category") \
        



    # ## groupby bot & find count
    # tayara_value_df = tayara_value_df \
    #     .withWatermark("timestamp", "60 seconds") \
    #     .groupBy(
    #         # window params -> timestamp, window interval, sliding interval
    #         func.window(
    #             func.col("timestamp"),
    #             "60 seconds",
    #             "30 seconds"
    #         ),
    #         func.col("category")
    #     ) \
    #     .agg(func.count("category").alias('counts')) \
        
    # Group by category and find the count of entries per category
    tayara_value_df = tayara_value_df \
        .groupBy(func.col("category")) \
        .agg(func.count("category").alias('counts'))





    # tayara_value_df = tayara_value_df.withColumn(
    #     "requested_by", 
    #     func.when(tayara_value_df.publisher == "Business", "Shop")\
    #     .otherwise("Individual")
    # ) \
    # .select(
    #     "window",
    #     "requested_by", 
    #     "counts"
    # )
    # tayara_value_df.printSchema()

    tayara_value_df = tayara_value_df.withColumn(
        "requested_by", 
        func.lit("fffrfr")  # Set requested_by to a constant string 'fffrfr'
    )

    tayara_value_df = tayara_value_df.select(
        "category", 
        "counts", 
        "requested_by"  # Only include the requested_by column once
    )
    tayara_value_df.printSchema()

    
    # tayara_value_df \
    #     .writeStream \
    #     .format("console") \
    #     .trigger(processingTime='1 second') \
    #     .outputMode("append") \
    #     .option("truncate", "false") \
    #     .start() \
    #     .awaitTermination()

    tayara_value_df \
    .writeStream \
    .format("console") \
    .trigger(processingTime='1 second') \
    .outputMode("update") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()






    

