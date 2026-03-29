import duckdb

con = duckdb.connect("data/nyc_taxi.duckdb")

# print(con.execute("SELECT * FROM stg_taxi_zones LIMIT 10").fetchdf())
# print(con.execute("SELECT * FROM stg_yellow_trips LIMIT 10").fetchdf())
print(con.execute("DESC TABLE int_trips_enriched").fetchdf())