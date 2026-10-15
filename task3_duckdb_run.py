# AI-ASSISTED: A1
"""
scripts/task3_duckdb_run.py
Usage:
    python scripts/task3_duckdb_run.py --trips data/taxi_2m.parquet --zones data/taxi_zone_lookup.csv
"""

import argparse
import time

import duckdb

from machine_info import TOOL_VERSIONS, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout

QUERY = """
SELECT
    z.Borough,
    z.Zone,
    ROUND(SUM(t.fare_amount), 2) AS total_fare
FROM read_parquet('{trips}') AS t
JOIN read_csv_auto('{zones}') AS z
    ON t."PULocationID" = z.LocationID
WHERE t.fare_amount >= 0
GROUP BY z.Borough, z.Zone
ORDER BY total_fare DESC, z.Zone ASC
LIMIT 5
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--trips", default="data/taxi_2m.parquet")
    parser.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    parser.add_argument("--timings-out", default="results/timings.csv")
    args = parser.parse_args()

    con = duckdb.connect()
    sql = QUERY.format(trips=args.trips, zones=args.zones)

    def run_once():
        start = time.perf_counter()
        con.execute(sql).fetchall()  # materializes the full top-5 result
        return time.perf_counter() - start

    results = measure_repeated(
        measured_fn=lambda: run_with_timeout(run_once),
        warmup_fn=lambda: run_with_timeout(run_once),
    )
    print("\nResults:", results)

    # Show the actual top-5 result once, outside the timed region.
    print("\nTop 5 zones:")
    for row in con.execute(sql).fetchall():
        print(row)

    # --- Record every measured run into results/timings.csv ---
    input_mb = file_size_mb(args.trips)
    timing_rows = [
        {
            "system": "duckdb",
            "task": "task3",
            "condition": "join_top5",
            "dataset_rows": 2_000_000,
            "input_format": "parquet",
            "input_mb": input_mb,
            "run_number": run_number,
            "time_seconds": round(elapsed, 4),
            "status": status,
            "tool_version": TOOL_VERSIONS["duckdb"],
        }
        for run_number, elapsed, status in results
    ]
    append_timing_rows(timing_rows, args.student_id, args.timings_out)
    print(f"Appended {len(timing_rows)} rows to {args.timings_out}")


if __name__ == "__main__":
    main()
