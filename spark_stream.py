from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json, avg
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import shutil
import os

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "test_topic"

# Absolute Delta path (same as reader)
DELTA_PATH = "<absolute or local path whatever works for you>"
CHECKPOINT_PATH = "<absolute or local path whatever works for you>"


def main():
    print("Starting Spark streaming job...")

    # Clean up old Delta + checkpoint dirs (local Windows paths, not file://)
    for path in [
        "C:/UpScale/AgenticAI/stock-analyzer/stream-delta-scd/deltaquery/agg_by_country",
        "C:/UpScale/AgenticAI/stock-analyzer/stream-delta-scd/deltaquery/_checkpoints/agg_by_country",
    ]:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Deleted old folder: {path}")

    # Spark session with Delta support
    spark = (
        SparkSession.builder
        .appName("DeltaLakeStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
        .config("spark.driver.host", "127.0.0.1")   # 👈 important fix
        .master("local[*]")                         # 👈 force local mode
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    print("Spark session created and configured.")

    # Schema to parse Kafka JSON
    schema = (
        StructType()
        .add("name", StringType())
        .add("age", IntegerType())
        .add("country", StringType())
        .add("event_time", TimestampType())
    )

    # Read from Kafka
    print(f"Reading from Kafka topic: {TOPIC_NAME}")
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .load()
    )
    print("Kafka stream read successfully.")

    # Parse JSON
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    json_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Windowed aggregation
    agg_df = (
        json_df.withWatermark("event_time", "3 seconds")
        .groupBy(window(col("event_time"), "15 seconds", "5 seconds"), col("country"))
        .agg(avg("age").alias("avg_age"))
    )

    # Write to Delta
    print(f"Writing streaming data to Delta table at: {DELTA_PATH}")
    query = (
        agg_df.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .outputMode("append")
        .start(DELTA_PATH)
    )

    print("Spark streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
