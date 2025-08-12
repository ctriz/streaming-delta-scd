from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,current_timestamp, avg, window
from pyspark.sql.types import StructType, StringType, IntegerType
from cassandra.cluster import Cluster
from kafka.admin import KafkaAdminClient, NewTopic
import time

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "test_topic"
CASSANDRA_KEYSPACE = "testks"

# We now have two tables for different purposes
CASSANDRA_PEOPLE_TABLE = "people"
CASSANDRA_STATS_TABLE = "age_stats"

# The IP address of Windows host machine, found using `ipconfig`.
CASSANDRA_HOST = "localhost" 

# --- Kafka Topic Management ---
def recreate_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)

    # Delete topic if exists
    existing_topics = admin_client.list_topics()
    if TOPIC_NAME in existing_topics:
        print(f"Deleting existing topic: {TOPIC_NAME}")
        admin_client.delete_topics([TOPIC_NAME])
        time.sleep(2)  # Wait for Kafka to fully delete the topic

    # Create topic
    print(f"Creating topic: {TOPIC_NAME}")
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    admin_client.create_topics([topic])
    admin_client.close()
    print(f"Topic {TOPIC_NAME} ready.")

# --- Cassandra Table Management ---

def recreate_cassandra_tables():
    """
    Connects to Cassandra and recreates two keyspace and tables with the correct schemas.
    """
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.execute(f"DROP KEYSPACE IF EXISTS {CASSANDRA_KEYSPACE}")
    session.execute(f"""
        CREATE KEYSPACE {CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)


    # Table to store filtered and joined people data
    session.execute(f"""
        CREATE TABLE {CASSANDRA_PEOPLE_TABLE} (
            id INT PRIMARY KEY,
            fname TEXT,
            age INT,
            country TEXT,
            city TEXT
        )
    """)

    # Table to store aggregated age statistics
    session.execute(f"""
        CREATE TABLE {CASSANDRA_STATS_TABLE} (
            window_start TIMESTAMP PRIMARY KEY,
            average_age FLOAT
        )
    """)
    cluster.shutdown()
    print("Cassandra tables ready.")


# --- Main Spark Streaming Job ---
def main(): 
    """
    Main function to run the Spark streaming job with advanced features.
    """
    recreate_kafka_topic()
    recreate_cassandra_tables()

    spark = SparkSession.builder \
        .appName("AdvancedSparkStreaming") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_NAME) \
        .load()
    
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # Define the schema to match the Kafka producer
    schema = StructType() \
        .add("id", IntegerType()) \
        .add("fname", StringType()) \
        .add("age", IntegerType()) \
        .add("country", StringType()) \
        
    
    # Parse the JSON and add a timestamp for windowing
    json_df = value_df.withColumn("data", from_json(col("json_str"), schema)) \
        .select("data.*") \
        .withColumn("timestamp", current_timestamp())

    # --- Feature 1: Filtering and Joining ---
    # Create a static DataFrame for the join
    locations_df = spark.createDataFrame([
        ("US", "New York"),
        ("UK", "London"),
        ("FR", "Paris"),
        ("JP", "Tokyo"),
        ("AU", "Sydney"),
        ("GR", "Berlin"),
        ("EG", "Cairo"),
    ], ["country", "city"])

    # Filter for people over 30 and join with the locations DataFrame
    filtered_and_joined_df = json_df.filter(col("age") > 30).join(locations_df, "country")

    # Write the filtered and joined results to the 'people' Cassandra table
    people_query = filtered_and_joined_df.select("id", "fname", "age", "country", "city").writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=CASSANDRA_PEOPLE_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode("append") \
            .save()) \
        .option("checkpointLocation", "C:/spark/checkpoints/people_data") \
        .start()

    # --- Feature 2: Aggregation with Windowing ---
    # Group by a 10-second window and calculate the average age
    windowed_avg_age_df = json_df.withWatermark("timestamp", "10 seconds") \
        .groupBy(window(col("timestamp"), "10 seconds", "5 seconds")) \
        .agg(avg("age").alias("average_age")) \
        .select(col("window.start").alias("window_start"), "average_age")

    # Write the aggregated results to the 'age_stats' Cassandra table
    stats_query = windowed_avg_age_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=CASSANDRA_STATS_TABLE, keyspace=CASSANDRA_KEYSPACE) \
            .mode("append") \
            .save()) \
        .option("checkpointLocation", "C:/spark/checkpoints/age_stats") \
        .start()

    # A single awaitTermination() call keeps all streaming queries running
    spark.streams.awaitAnyTermination()

    
    # --- Debugging Sink ---
    # This console sink will show you if any data is making it through the filter and join.
    # It is separate from the Cassandra queries and will help you confirm data is being processed.
    console_query = filtered_and_joined_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    # The program will now wait for any of the streaming queries to terminate.
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
