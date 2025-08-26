#  Real-Time Streaming with Kafka, Spark & Delta Lake

This project demonstrates a real-time data streaming pipeline using Kafka, Spark Structured Streaming, and Delta Lake, with support for schema evolution, windowed aggregations, watermarking, and SCD Type 2.  

---

## Key Features
- **Part A: Basic Streaming Pipeline**
  - Kafka producer publishes streaming data.  
  - Spark Structured Streaming client ingests and writes to Delta tables.  
  - Implements *windowed aggregation, watermarking, and schema evolution*.  
  - Consumers (Spark / DuckDB) read results from Delta Lake.  

- **Part B: Slowly Changing Dimensions (SCD Type 2)**
  - Kafka producer publishes random updates + new records.  
  - Multiple producers simulate changes in dimensions.  
  - Spark client ingests data into Delta tables with *SCD Type 2 handling*.  
  - Delta tables can be queried using Spark or DuckDB.  
---

## Tech Stack
- **Apache Kafka**   
- **Apache Spark Structured Streaming**  
- **Delta Lake**   
- **DuckDB** (for lightweight querying)  
- **Docker** (containerized setup)  

## Usage

```bash
docker compose up --build
./run_spark.ps1 producer.py
./run_spark.ps1 spark_stream.py
./run_spark.ps1 read_delta_spark.py (alternatively read_delta_duckDB.py)

docker compose down && docker compose up -build
./run_spark.ps1 producer_scd2.py
./run_spark.ps1 spark_stream_scd2.py
./run_spark.ps1 read_delta_scd2.py 
```

## Upcoming Enhancements

-   **Airflow / Prefect Orchestration** for end-to-end pipeline automation.
    
-   **Fully Dockerized Deployment** (race-proof startup sequencing for Kafka + Spark)
