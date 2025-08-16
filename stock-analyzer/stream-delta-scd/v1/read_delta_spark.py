from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Inspect Delta Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to your Delta table
delta_path = "./delta-table"

print("\n=== Data ===")
df = spark.read.format("delta").load(delta_path)
df.show()

print("\n=== Schema ===")
df.printSchema()

spark.stop()
