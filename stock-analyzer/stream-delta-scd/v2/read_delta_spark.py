from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from delta.tables import DeltaTable
from datetime import datetime
import os
import shutil

# Path to the Delta table
delta_table_path = "./delta-table"
query_output_dir = "./deltaquery"
# Ensure the output directory exists
if not os.path.exists(query_output_dir):
    os.makedirs(query_output_dir)
    print(f"Created output directory: {query_output_dir}")


# 🧹 Cleanup old results
if os.path.exists(query_output_dir):
    shutil.rmtree(query_output_dir)
    print(f"Deleted previous query output folder: {query_output_dir}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Inspect Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()



# Read Delta Table
df_latest = spark.read.format("delta").load(delta_table_path)

print("\n=== Delta Table History ===")
delta_table = DeltaTable.forPath(spark, delta_table_path)
delta_table.history().show(truncate=False)

# Print filtered data for CA or UK
print("\n=== Records where country is CA or UK ===")
df_latest.filter(col("country").isin("CA", "UK")).show()

# Print avg age for IN in console
avg_age_latest = df_latest.filter(col("country") == "IN") \
                          .agg(avg(col("age")).alias("avg_age")) \
                          .collect()[0]["avg_age"]

print(f"\nAverage age for country IN: {avg_age_latest}")

# Save avg age for IN to ./deltaquery/timestamp/
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = os.path.join(query_output_dir, f"avg_age_IN_{timestamp_str}")

df_avg_in = df_latest.filter(col("country") == "IN") \
                     .groupBy("country").avg("age").alias("avg_age")
df_avg_in = df_avg_in.withColumnRenamed("avg(age)", "avg_age")


df_avg_in.write.format("delta").mode("overwrite").save(output_path)

print(f"\nSaved average age for IN to: {output_path}")

# Stop Spark cleanly
spark.stop()
