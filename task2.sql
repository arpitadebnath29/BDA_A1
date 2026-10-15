-- AI-ASSISTED: A1
-- duckdb/task2.sql
--
-- Task 2 (DuckDB version). See duckdb/task2_explain.py for the script that
-- actually runs this, times it (warm-up + 3 measured runs) and saves the
-- explained output to duckdb/task2_explain.txt.

-- Query A: all columns
SELECT *
FROM read_parquet('data/taxi_2m.parquet')
WHERE "PULocationID" = 161
  AND fare_amount > 30.00;

-- Query B: 4 columns only
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount
FROM read_parquet('data/taxi_2m.parquet')
WHERE "PULocationID" = 161
  AND fare_amount > 30.00;
