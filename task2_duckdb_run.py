# AI-ASSISTED: A1
"""
scripts/task2_duckdb_run.py

Usage:
    python scripts/task2_duckdb_run.py --input data/taxi_2m.parquet

Only run this locally -- it performs real timed execution.
"""

import argparse
import time
import duckdb

from machine_info import TOOL_VERSIONS, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout

QUERY_A = """
SELECT *
FROM read_parquet('{input}')
WHERE "PULocationID" = 161
  AND fare_amount > 30.00
"""

QUERY_B = """
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount
FROM read_parquet('{input}')
WHERE "PULocationID" = 161
  AND fare_amount > 30.00
"""


def time_query(con, sql, label):
    """Runs 3 measured runs, each with 900s timeout protection and the
    timed-out-combination-skip logic from machine_info.measure_repeated().
    Returns [(run_number, elapsed, status), ...]."""
    def run_once():
        start = time.perf_counter()
        con.execute(sql).fetchall()  # fetchall() materializes ALL rows
        return time.perf_counter() - start

    print(f"[{label}]")
    return measure_repeated(measured_fn=lambda: run_with_timeout(run_once))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--input", default="data/taxi_2m.parquet")
    parser.add_argument("--explain-out", default="duckdb/task2_explain.txt")
    parser.add_argument("--timings-out", default="results/timings.csv")
    args = parser.parse_args()

    con = duckdb.connect()
    sql_a = QUERY_A.format(input=args.input)
    sql_b = QUERY_B.format(input=args.input)

    print("Warm-up run (not measured)...")
    try:
        run_with_timeout(lambda: con.execute(sql_a).fetchall())
    except Exception as e:
        print(f"  warm-up TIMED OUT or ERRORED ({e}) -- continuing to measured runs")

    results_a = time_query(con, sql_a, "Query A (all columns)")
    results_b = time_query(con, sql_b, "Query B (4 columns)")

    print("\nQuery A results:", results_a)
    print("Query B results:", results_b)

    # Save EXPLAIN for BOTH queries so we can compare which columns are
    # read (Projections) and whether the filter reaches the scan node.
    explain_a = con.execute(f"EXPLAIN {sql_a}").fetchall()
    explain_b = con.execute(f"EXPLAIN {sql_b}").fetchall()
    with open(args.explain_out, "w") as f:
        f.write("========== QUERY A (all columns) ==========\n")
        for row in explain_a:
            f.write(str(row[-1]) + "\n")
        f.write("\n========== QUERY B (4 columns) ==========\n")
        for row in explain_b:
            f.write(str(row[-1]) + "\n")
    print(f"\nSaved DuckDB EXPLAIN output (Query A and B) to {args.explain_out}")

    # --- Record every measured run into results/timings.csv ---
    input_mb = file_size_mb(args.input)
    rows = []
    for condition, results in [("query_a", results_a), ("query_b", results_b)]:
        for run_number, elapsed, status in results:
            rows.append({
                "system": "duckdb",
                "task": "task2",
                "condition": condition,
                "dataset_rows": 2_000_000,
                "input_format": "parquet",
                "input_mb": input_mb,
                "run_number": run_number,
                "time_seconds": round(elapsed, 4),
                "status": status,
                "tool_version": TOOL_VERSIONS["duckdb"],
            })
    append_timing_rows(rows, args.student_id, args.timings_out)
    print(f"Appended {len(rows)} rows to {args.timings_out}")


if __name__ == "__main__":
    main()
