# Streaming example with spark 

This project uses Kafka in a docker form to initialize. You can use a standalone too. 
The project has two functionalities - 
Part A: 
An ordinary producer publsihing data to the Kafka topic in a streaming fashion.
A spark client to read and write to a delta table. Employs windows aggregaation, with watermarking and schema evolution.
Another spark client to read the delta table. 

Part B:
Another producer publsihing data to the Kafka topic in a streaming fashion in a random fashion simulating new and updated records. 
Couple of producers to publish the data with slowly changing dimensions.
A spark client to read and write to a delta table capturing the SCD2.

I have given example of both Spark and DuckDB to read the DeltaLake/Parquet output.
Also given couple of bash commands to clean up the files/env and run the spark programs wrapping the dependent jars

## TechStack Used

- Kafka
- Spark  
- Delta Lake
- Docker

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

## Upcoming

Airflow/Prefect to orchestrate this entire manual flow!
Alternatively, everything into a dockerized container. On Windows + Docker the “depends_on” only gates container start, not readiness, so the producer or Spark usually comes up before Kafka is truly ready or before the topic exists. Working on making the stack race‑proof.
