# inspect_scd.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("InspectSCD") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "./delta_scd"
df = spark.read.format("delta").load(delta_path)

df.show(truncate=False)
