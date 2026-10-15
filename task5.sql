-- AI-ASSISTED: A1
-- duckdb/task5.sql
--
-- Task 5 (DuckDB) query text -- run 5 times on the SAME connection using
-- scripts/task5_duckdb_run.py (a bare .sql file can't loop/time itself).

SELECT
    "PULocationID",
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 4) AS avg_fare
FROM read_parquet('data/taxi_1m.parquet')
WHERE "PULocationID" IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY "PULocationID";
