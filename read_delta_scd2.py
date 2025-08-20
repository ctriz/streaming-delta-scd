from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadSCD2Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

delta_path = "file:///C:/UpScale/AgenticAI/stock-analyzer/stream-delta-scd/deltaquery/employees_scd2"

df = spark.read.format("delta").load(delta_path)

print("\n=== Current Records (is_current = true) ===")
df.filter("is_current = true").show(truncate=False)

print("\n=== Historical Records (is_current = false) ===")
df.filter("is_current = false").show(truncate=False)

spark.stop()
