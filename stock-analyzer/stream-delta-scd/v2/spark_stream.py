from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType,IntegerType, DoubleType
import shutil
import os

# Use Docker service names for connectivity
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "test_topic" 

def main():

    """
    Main function to run the Spark streaming job writing to Delta Lake.
    """
    print("Starting Spark streaming job...")

    # Clean up previous Delta table & checkpoint data
    delta_table_path = "./delta-table"
    checkpoint_path = "./checkpoints"
    
    for path in [delta_table_path, checkpoint_path]:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Deleted old folder: {path}")

    #create Spark session with Delta Lake support
    spark = SparkSession.builder \
        .appName("DeltaLakeStreaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("Spark session created and configured.")

    # Read from Kafka topic as a stream
    print(f"Reading from Kafka topic: {TOPIC_NAME}")
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_NAME) \
        .load()
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # Define the schema to match the Kafka producer's JSON structure.
    schema = StructType() \
        .add("id", IntegerType())   \
        .add("fname", StringType()) \
        .add("age", IntegerType()) \
        .add("country", StringType())     
    json_df = value_df.withColumn("data", from_json(col("json_str"), schema)) \
        .select("data.*") \
        .withColumn("timestamp", current_timestamp())
    
    # --- Write to Delta Lake ---

    # The Delta table will be created at this path inside the container.
    
    print(f"Writing streaming data to local Delta table at: ./delta-table")
    query = json_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints") \
        .start("./delta-table")    
    
    print("Spark streaming query started. Waiting for termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
    
