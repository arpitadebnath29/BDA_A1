# AI-ASSISTED: A1
"""
scripts/task5_duckdb_run.py


Usage:
    python scripts/task5_duckdb_run.py --input data/taxi_1m.parquet
"""

import argparse
import time

import duckdb

from machine_info import TOOL_VERSIONS, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout

QUERY = """
SELECT
    "PULocationID",
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 4) AS avg_fare
FROM read_parquet('{input}')
WHERE "PULocationID" IS NOT NULL
  AND fare_amount IS NOT NULL
  AND fare_amount >= 0
GROUP BY "PULocationID"
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--input", default="data/taxi_1m.parquet")
    parser.add_argument("--n-runs", type=int, default=5)
    parser.add_argument("--timings-out", default="results/timings.csv")
    args = parser.parse_args()

    con = duckdb.connect()  # one connection, reused for all 5 runs
    sql = QUERY.format(input=args.input)

    def run_once():
        start = time.perf_counter()
        con.execute(sql).fetchall()
        return time.perf_counter() - start

    # No separate warm-up here -- Task 5's spec calls for 5 runs directly,
    # not the standard warm-up+3 protocol. Timeout + skip-on-timeout
    # protection still applies via measure_repeated().
    results = measure_repeated(measured_fn=lambda: run_with_timeout(run_once), n_runs=args.n_runs)

    print("\nResults:", results)

    # --- Record every measured run into results/timings.csv ---
    input_mb = file_size_mb(args.input)
    rows = [
        {
            "system": "duckdb", "task": "task5", "condition": "duckdb_repeat",
            "dataset_rows": 1_000_000, "input_format": "parquet", "input_mb": input_mb,
            "run_number": run_number, "time_seconds": round(elapsed, 4), "status": status,
            "tool_version": TOOL_VERSIONS["duckdb"],
        }
        for run_number, elapsed, status in results
    ]
    append_timing_rows(rows, args.student_id, args.timings_out)
    print(f"Appended {len(rows)} rows to {args.timings_out}")


if __name__ == "__main__":
    main()
