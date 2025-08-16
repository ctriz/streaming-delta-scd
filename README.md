# Streaming example with spark 

This project uses Kafka in a docker form to listen to topics. 
A producer publishes the data with slowly changing dimensions.
A streaming client based on PySpark reads the Kafka topic and saves the data to a Delta Lake table. 
I have given example of both Spark and DuckDB to read the DeltaLake/PArquet output

## TechStack Used

- Kafka
- Spark  
- Delta Lake
- Docker

## Usage

```bash
docker compose up --build
python producer.py
spark submit spark_streaming.py
python producer_scd.py
python read_delta_spark.py (alternatively read_delta_duckDB.py)
python inspect_scd.py 
```

## Upcoming

Airflow/Prefect to orchestrate this entire manual flow!
