# spark_stream_scd2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit, sha2, concat_ws, row_number
)
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ---------- CONFIG ----------
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME      = "test_topic_scd2"

# Absolute Windows file URI
DELTA_BASE      = "<absolute or local path whatever works for you>"
DELTA_PATH      = f"{DELTA_BASE}/employees_scd2"
CHECKPOINT_DIR  = f"{DELTA_BASE}/_checkpoints/employees_scd2"

# SCD2 columns
START_COL = "start_time"
END_COL   = "end_time"
CUR_COL   = "is_current"
OPEN_END  = "9999-12-31 23:59:59"

# Business key and tracked attributes
BUSINESS_KEY   = "name"                # demo key
TRACKED_ATTRS  = ["age", "country"]
ALL_COLS       = [BUSINESS_KEY] + TRACKED_ATTRS + ["event_time"]

# ---------- SCHEMA ----------
schema = (
    StructType()
      .add("name",    StringType())
      .add("age",     IntegerType())
      .add("country", StringType())
      .add("event_time", StringType())   # ingest as string; cast later
)

# ---------- SPARK ----------
spark = (
    SparkSession.builder
        .appName("DeltaLakeSCD2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------- READ KAFKA (stream) ----------
raw = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
)

# Parse JSON and convert event_time to timestamp
# Note: event_time is in UTC format
# Note: from_json requires the schema to be defined
# Note: we use selectExpr to rename the Kafka timestamp column
# Note: we use watermarking to handle late data
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts")
       .select(from_json(col("json"), schema).alias("d"), "kafka_ts")
       .select("d.*", "kafka_ts")
       .withColumn("event_ts", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
       .withWatermark("event_ts", "10 minutes")
)

def scd2_merge(batch_df, epoch_id: int):
    # batch_df is STATIC => window/row_number is allowed here
    if batch_df.isEmpty():
        return

    # pick the latest row per business key within this micro-batch
    # Note: we use row_number to ensure we get the latest event_ts + kafka_ts
    # Define window to partition by business key and order by event_ts and kafka_ts
    # Note: this is a static DataFrame operation
    # Define the window specification
    # Note: row_number is used to get the latest record per key
    w = Window.partitionBy(BUSINESS_KEY).orderBy(col("event_ts").desc(), col("kafka_ts").desc())
    latest = (
        batch_df.withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .drop("rn")
    )

    # compute a hash for tracked attrs to cheaply detect change
    # Note: concat_ws is used to concatenate attributes into a single string
    # Note: sha2 computes a hash of the concatenated string
    # Note: we cast all attributes to string to ensure consistent hashing
    # Note: attr_hash is used to detect changes in attributes

    attr_hash = sha2(concat_ws("||", *[col(c).cast("string") for c in TRACKED_ATTRS]), 256)
    staged = (
        latest
        .withColumn("attr_hash", attr_hash)
        .withColumn(START_COL, col("event_ts"))                 # start at event time
        .withColumn(END_COL, to_timestamp(lit(OPEN_END)))       # open-ended
        .withColumn(CUR_COL, lit(True))
    )

    # ensure Delta table exists (create with correct columns on first batch)
    # Note: isDeltaTable checks if the path is a Delta table
    # Note: staged.limit(0) creates an empty DataFrame with the correct schema
    # Note: we use overwrite mode to create the table if it doesn't exist
    if not DeltaTable.isDeltaTable(spark, DELTA_PATH):
        (staged.limit(0)
               .write.format("delta")
               .mode("overwrite")
               .save(DELTA_PATH))
        ## Create Delta table with correct schema

    delta_tbl = DeltaTable.forPath(spark, DELTA_PATH)

    # keys in this micro-batch
    # Note: this is used to find current active versions for these keys
    # Note: we use distinct to avoid duplicates in the keys DataFrame
    keys_df = staged.select(BUSINESS_KEY).distinct()

    # current active versions for those keys
    # Note: we filter to get only the current rows (CUR_COL = True)
    # Note: we select only the business key and tracked attributes
    current_df = (
        delta_tbl.toDF()
                 .join(keys_df, on=BUSINESS_KEY, how="inner")
                 .filter(col(CUR_COL) == True)
                 .select(BUSINESS_KEY, *TRACKED_ATTRS, "attr_hash", START_COL, END_COL, CUR_COL)
    )

    # join to detect new vs changed
    # Note: we use left join to keep all staged rows
    # Note: we use alias to avoid column name conflicts
    # Note: we use col to refer to columns in the joined DataFrame
    # Note: this will create nulls for new keys (no current version)
    # Note: we use alias to avoid column name conflicts 
    joined = staged.alias("s").join(current_df.alias("t"), on=BUSINESS_KEY, how="left")

    # new keys (no current version)
    # Note: we filter where the current version is null
    # Note: we select all columns from the staged DataFrame
    # Note: this will create new rows for new keys
    # Note: we use unionByName to ensure correct column order
    new_rows = joined.filter(col(f"t.{BUSINESS_KEY}").isNull()).select("s.*")

    # changed rows (hash differs)
    # Note: we filter where the current version is not null and the hash differs
    # Note: we select all columns from the staged DataFrame
    # Note: this will create new rows for changed keys
    # Note: we use unionByName to ensure correct column order
    # Note: attr_hash is used to detect changes in attributes
    changed_rows = joined.filter(
        col(f"t.{BUSINESS_KEY}").isNotNull() & (col("s.attr_hash") != col("t.attr_hash"))
    ).select("s.*")

    # 1) close current rows for changed keys (end at the new version's start)
    # Note: we select distinct business keys and start times to close
    # Note: we use alias to avoid column name conflicts
    # Note: we use merge to update the current rows
    ids_to_close = changed_rows.select(BUSINESS_KEY, START_COL).distinct()
    if not ids_to_close.isEmpty():
        delta_tbl.alias("t").merge(
            ids_to_close.alias("s"),
            f"t.{BUSINESS_KEY} = s.{BUSINESS_KEY} AND t.{CUR_COL} = true"
        ).whenMatchedUpdate(set={
            END_COL: f"s.{START_COL}",
            CUR_COL: "false"
        }).execute()

    # 2) insert new versions for new + changed
    # Note: we use unionByName to ensure correct column order
    # Note: we append the new and changed rows to the Delta table
    # Note: this will create new rows for new and changed keys
    # Note: we use mode("append") to add to the existing table
    # Note: we use unionByName to ensure correct column order
    inserts = new_rows.unionByName(changed_rows)
    if not inserts.isEmpty():
        inserts.write.format("delta").mode("append").save(DELTA_PATH)

# ---------- START STREAM ----------
q = (
    parsed.writeStream
          .foreachBatch(scd2_merge)   # all heavy logic done here (static DF)
          .option("checkpointLocation", CHECKPOINT_DIR)  # absolute path
          .outputMode("update")       # required with foreachBatch; sink is inside callback
          .start()
)
#Write to console for debugging
# q = (
#     parsed.writeStream    
#           .format("console")
#           .outputMode("append")  # append mode to show new rows
#           .option("truncate", "false")  # don't truncate long strings
#           .start()
# )

# ---------- WAIT FOR TERMINATION ----------
print("Streaming started. Press Ctrl+C to stop.")

# Wait for termination
q.awaitTermination()
