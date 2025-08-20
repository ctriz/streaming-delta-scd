from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

def main():
    # Create Spark session with Delta support
    spark = (
        SparkSession.builder
        .appName("InspectDeltaSpark")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.host", "127.0.0.1")   # Ensure driver binds locally
        .master("local[*]")                         # Run locally
        .getOrCreate()
    )

    # Absolute path to your Delta table
    delta_path = (
        "<absolute or local path whatever works for you>"
    )

    print("\n=== Delta Table History ===")
    delta_table = DeltaTable.forPath(spark, delta_path)
    history_df = delta_table.history()
    history_df.show(truncate=False)

    # Get latest snapshot
    df = spark.read.format("delta").load(delta_path)

    # Flatten window struct if present
    if "window" in df.columns:
        df = (
            df.withColumn("window_start", col("window.start"))
              .withColumn("window_end", col("window.end"))
              .drop("window")
        )

    print("\n=== Delta Table Data ===")
    df.printSchema()
    df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
