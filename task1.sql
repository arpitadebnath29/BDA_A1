--- AI-ASSISTED: A1
---duckdb/task1.sql
---Task 1 (DuckDB): For every non-null PULocationID, compute
---trip_count and avg_fare, using only rows where
---fare_amount IS NOT NULL AND fare_amount >= 0.


SELECT
    "PULocationID",
    COUNT(*)                AS trip_count,
    ROUND(AVG(fare_amount), 4) AS avg_fare
FROM read_csv_auto('data/taxi_1m.csv')
WHERE "PULocationID" IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY "PULocationID"
ORDER BY "PULocationID";
