import duckdb
import os

delta_path = "./deltaquery/agg_by_country"
parquet_glob = os.path.join(delta_path, "*.parquet")

con = duckdb.connect()

print("\n=== Schema (Parquet files) ===")
print(con.sql(f"DESCRIBE SELECT * FROM read_parquet('{parquet_glob}')").df())

print("\n=== Latest Data ===")
query = f"""
SELECT
    ("window").start AS window_start,
    ("window").end   AS window_end,
    country,
    avg_age
FROM read_parquet('{parquet_glob}')
ORDER BY ("window").start DESC
LIMIT 10
"""
print(con.sql(query).df())

con.close()
