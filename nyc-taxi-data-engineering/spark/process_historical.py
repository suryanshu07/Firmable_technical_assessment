from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    when,
    sum as _sum,
    avg,
    count,
    expr
)

# ------------------------------------------------------------
# 1. SPARK SESSION
# ------------------------------------------------------------
spark = SparkSession.builder \
    .appName("NYC Taxi Historical Processing") \
    .getOrCreate()

# ------------------------------------------------------------
# CONFIG (adjust as needed)
# ------------------------------------------------------------
INPUT_PATH = "s3://nyc-tlc/trip data/yellow_tripdata_*.parquet"  # or local path
ZONE_LOOKUP_PATH = "s3://path/to/taxi_zone_lookup.csv"
OUTPUT_PATH = "s3://your-bucket/nyc_taxi_agg_daily/"

# ------------------------------------------------------------
# 2. READ DATA
# ------------------------------------------------------------
df = spark.read.parquet(INPUT_PATH)

# ------------------------------------------------------------
# 3. CLEANING LOGIC (same as DBT staging)
# ------------------------------------------------------------
df_clean = df.select(
    col("VendorID").alias("vendor_id"),

    to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
    to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime"),

    col("passenger_count").cast("double"),
    col("trip_distance").cast("double"),

    col("fare_amount").cast("double"),
    col("tip_amount").cast("double"),
    col("total_amount").cast("double"),

    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id")
)

# ------------------------------------------------------------
# 4. DERIVED COLUMNS
# ------------------------------------------------------------
df_clean = df_clean.withColumn(
    "trip_duration_minutes",
    (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60
)

df_clean = df_clean.withColumn("pickup_year", year("pickup_datetime")) \
                   .withColumn("pickup_month", month("pickup_datetime")) \
                   .withColumn("pickup_day", dayofmonth("pickup_datetime")) \
                   .withColumn("pickup_hour", hour("pickup_datetime"))

# ------------------------------------------------------------
# 5. FILTER INVALID RECORDS (same as intermediate layer)
# ------------------------------------------------------------
df_filtered = df_clean.filter(
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("passenger_count") > 0) &
    (col("trip_duration_minutes") >= 1) &
    (col("trip_duration_minutes") <= 180)
)

# ------------------------------------------------------------
# 6. CACHE (important for large dataset reuse)
# ------------------------------------------------------------
df_filtered.cache()

# ------------------------------------------------------------
# 7. DAILY AGGREGATION (agg_daily_revenue equivalent)
# ------------------------------------------------------------
df_daily = df_filtered.groupBy(
    "pickup_year",
    "pickup_month",
    "pickup_day"
).agg(
    count("*").alias("total_trips"),
    _sum("fare_amount").alias("total_fare"),
    avg("fare_amount").alias("avg_fare"),
    _sum("tip_amount").alias("total_tips"),
    _sum("total_amount").alias("total_revenue"),
    (
        _sum("tip_amount") / _sum("total_amount") * 100
    ).alias("tip_rate_percent")
)

# ------------------------------------------------------------
# 8. REPARTITION (critical for write performance)
# ------------------------------------------------------------
# Repartition by year/month to:
# - Reduce shuffle skew
# - Ensure balanced output files
# - Align with partitioning strategy

df_daily = df_daily.repartition("pickup_year", "pickup_month")

# ------------------------------------------------------------
# 9. WRITE PARTITIONED OUTPUT
# ------------------------------------------------------------
df_daily.write \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .parquet(OUTPUT_PATH)

# ------------------------------------------------------------
# DONE
# ------------------------------------------------------------
spark.stop()