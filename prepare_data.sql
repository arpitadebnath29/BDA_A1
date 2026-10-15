COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 100000)
TO 'taxi_100k.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 100000)
TO 'taxi_100k.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 500000)
TO 'taxi_500k.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 500000)
TO 'taxi_500k.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 1000000)
TO 'taxi_1m.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 1000000)
TO 'taxi_1m.csv' (FORMAT CSV, HEADER TRUE);

COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 2000000)
TO 'taxi_2m.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM read_parquet('yellow_tripdata_2026-01.parquet') LIMIT 2000000)
TO 'taxi_2m.csv' (FORMAT CSV, HEADER TRUE);

-- Verify the row count of every generated file before starting the measured
-- experiments. If your generated row counts differ from the values above,
-- fix the data-preparation step before continuing. Required by the
-- assignment itself.

SELECT 'taxi_100k.csv'     AS file, COUNT(*) AS row_count FROM read_csv_auto('taxi_100k.csv')
UNION ALL
SELECT 'taxi_100k.parquet', COUNT(*) FROM read_parquet('taxi_100k.parquet')
UNION ALL
SELECT 'taxi_500k.csv',     COUNT(*) FROM read_csv_auto('taxi_500k.csv')
UNION ALL
SELECT 'taxi_500k.parquet', COUNT(*) FROM read_parquet('taxi_500k.parquet')
UNION ALL
SELECT 'taxi_1m.csv',       COUNT(*) FROM read_csv_auto('taxi_1m.csv')
UNION ALL
SELECT 'taxi_1m.parquet',   COUNT(*) FROM read_parquet('taxi_1m.parquet')
UNION ALL
SELECT 'taxi_2m.csv',       COUNT(*) FROM read_csv_auto('taxi_2m.csv')
UNION ALL
SELECT 'taxi_2m.parquet',   COUNT(*) FROM read_parquet('taxi_2m.parquet');

