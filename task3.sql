-- AI-ASSISTED: A1
-- duckdb/task3.sql
--
-- Task 3 (DuckDB version): join taxi_2m.parquet with taxi_zone_lookup.csv,
-- filter fare_amount >= 0, sum fare_amount per Borough + Zone, top 5 by
-- total_fare (ties broken by Zone ascending).
-- Run with:
--   duckdb -c ".read duckdb/task3.sql"

SELECT
    z.Borough,
    z.Zone,
    ROUND(SUM(t.fare_amount), 2) AS total_fare
FROM read_parquet('data/taxi_2m.parquet') AS t
JOIN read_csv_auto('data/taxi_zone_lookup.csv') AS z
    ON t."PULocationID" = z.LocationID
WHERE t.fare_amount >= 0
GROUP BY z.Borough, z.Zone
ORDER BY total_fare DESC, z.Zone ASC
LIMIT 5;


