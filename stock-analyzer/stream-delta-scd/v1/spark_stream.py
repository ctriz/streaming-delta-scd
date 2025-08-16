from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

# ✅ Create SparkSession with Delta Lake support
spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ✅ Define simple schema for Kafka messages
schema = StructType([
    StructField("id", IntegerType()),
    StructField("timestamp", DoubleType()),
    StructField("value", DoubleType())
])

# ✅ Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sample-topic") \
    .load()

# ✅ Parse the JSON value into columns
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
              .select("data.*")

# ✅ Write the data to a local Delta table
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoints") \
    .start("./delta-table")

query.awaitTermination()
