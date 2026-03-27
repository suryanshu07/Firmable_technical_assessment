# ingest_duckdb.py

import duckdb
import numpy

con = duckdb.connect("data/nyc_taxi.duckdb")

# Create raw table directly from parquet
# con.execute("""
# CREATE OR REPLACE TABLE yellow_tripdata AS
# SELECT * FROM read_parquet('data/yellow_tripdata_2023-*.parquet');
# """)

# Validate
# count = con.execute("SELECT COUNT(*) FROM yellow_tripdata").fetchone()[0]
# print(f"Loaded rows: {count}")

# Show tables
print(con.execute("SHOW TABLES").fetchall())

# Preview data
# df = con.execute("SELECT * FROM yellow_tripdata LIMIT 10").fetchdf()
# df = con.execute("SELECT DISTINCT DATE(tpep_pickup_datetime) FROM yellow_tripdata LIMIT 10;").fetchdf()
df = con.execute("SELECT DISTINCT EXTRACT(YEAR FROM tpep_pickup_datetime) AS year FROM yellow_tripdata ORDER BY 1 DESC;").fetchdf()
print(df)