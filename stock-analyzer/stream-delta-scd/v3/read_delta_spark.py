# inspect_delta_spark.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Path to your Delta table
delta_path = "./delta_output"

# Create SparkSession with Delta support
spark = (
    SparkSession.builder
    .appName("InspectDeltaSpark")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Get Delta table history to find latest version
print("\n=== Delta Table History ===")
history_df = spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`")
history_df.show(truncate=False)

latest_version = history_df.agg({"version": "max"}).collect()[0][0]
print(f"\nLatest Delta table version: {latest_version}")

# Read only the latest version
print("\n=== Reading Latest Delta Table Version ===")
df = (
    spark.read.format("delta")
    .option("versionAsOf", latest_version)
    .load(delta_path)
)

print("\n=== Schema (Original) ===")
df.printSchema()

# Flatten window struct if present
if "window" in df.columns:
    df = df.withColumn("window_start", col("window.start")) \
           .withColumn("window_end", col("window.end")) \
           .drop("window")

print("\n=== Schema (Flattened) ===")
df.printSchema()

print("\n=== Sample Data (Latest 10 rows) ===")
if "window_end" in df.columns:
    df.orderBy(col("window_end").desc()).show(10, truncate=False)
else:
    df.show(10, truncate=False)

spark.stop()
