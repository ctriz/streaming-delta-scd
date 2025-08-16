# streaming_scd.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("Kafka-SCD2-Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "employees"
delta_path = "./delta_scd"

# Schema of incoming Kafka value
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("event_time", StringType(), True)
])

# Read from Kafka
raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

# Add SCD2 metadata
incoming = parsed.withColumn("start_date", current_timestamp()) \
                 .withColumn("end_date", lit(None).cast("timestamp")) \
                 .withColumn("is_current", lit(True))

def scd2_merge(microBatchDF, batchId):
    if not microBatchDF.isEmpty():
        if DeltaTable.isDeltaTable(spark, delta_path):
            delta_table = DeltaTable.forPath(spark, delta_path)
            # Merge on natural key (here "name")
            (delta_table.alias("t")
                .merge(
                    microBatchDF.alias("s"),
                    "t.name = s.name AND t.is_current = true"
                )
                .whenMatchedUpdate(
                    condition="t.age <> s.age OR t.country <> s.country",
                    set={
                        "end_date": "current_timestamp()",
                        "is_current": "false"
                    }
                )
                .whenNotMatchedInsertAll()
                .execute())
        else:
            # First write
            microBatchDF.write.format("delta").mode("overwrite").save(delta_path)

query = incoming.writeStream \
    .foreachBatch(scd2_merge) \
    .outputMode("update") \
    .option("checkpointLocation", "./chk_scd") \
    .start()

query.awaitTermination()
