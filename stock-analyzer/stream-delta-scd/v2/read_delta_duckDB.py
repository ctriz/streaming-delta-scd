import os
import glob
import re
import duckdb

# Base output folder
base_dir = "./deltaquery"

# Regex for timestamped folder names: avg_age_IN_YYYYMMDD_HHMMSS
folder_pattern = re.compile(r"^avg_age_IN_\d{8}_\d{6}$")

# Filter only matching folders
subfolders = [
    f.path for f in os.scandir(base_dir)
    if f.is_dir() and folder_pattern.match(os.path.basename(f.path))
]

if not subfolders:
    raise FileNotFoundError(f"No matching subfolders found in {base_dir}")

# Sort by modification time (latest first)
latest_folder = max(subfolders, key=os.path.getmtime)

print(f" Using latest folder: {latest_folder}")

# Find all Parquet files
parquet_files = glob.glob(os.path.join(latest_folder, "*.parquet"))

if not parquet_files:
    raise FileNotFoundError(f"No Parquet files found in {latest_folder}")

# Query Parquet with DuckDB
con = duckdb.connect()
df = con.execute(f"SELECT * FROM parquet_scan('{latest_folder}/*.parquet')").fetchdf()

print("Data from latest parquet output:")
print(df)
