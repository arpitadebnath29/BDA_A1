# AI-ASSISTED: A1
"""
spark/task2.py
Task 2 (Spark version): read taxi_2m.parquet, filter
PULocationID = 161 AND fare_amount > 30.00.

Query A: all columns.
Query B: only tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance,
         fare_amount.

Usage:
    python spark/task2.py --input data/taxi_2m.parquet
"""

import argparse
import io
import os
import sys
import time
from contextlib import redirect_stdout

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# scripts/machine_info.py holds the shared timings.csv column list, machine
# config, and append helper -- used so every task's timing script agrees on
# the exact same machine info instead of drifting apart.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from machine_info import TOOL_VERSIONS, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout


def build_spark():
    return (
        SparkSession.builder
        .appName("Task2-FilterProject")
        .master("local[*]")
        .getOrCreate()
    )


def query_a(df):
    """All columns."""
    return df.filter((F.col("PULocationID") == 161) & (F.col("fare_amount") > 30.00))


def query_b(df):
    """Only 4 columns."""
    return query_a(df).select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
    )


def time_query(build_query_fn, df, label):
    """Runs 3 measured runs of build_query_fn(df), each with the 900s
    timeout protection and timed-out-combination-skip logic from
    machine_info.measure_repeated(). Returns [(run_number, elapsed, status), ...]."""
    def run_once():
        start = time.perf_counter()
        build_query_fn(df).count()  # forces full materialization
        return time.perf_counter() - start

    print(f"[{label}]")
    return measure_repeated(measured_fn=lambda: run_with_timeout(run_once))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--input", default="data/taxi_2m.parquet")
    parser.add_argument("--explain-out", default="spark/task2_explain.txt")
    parser.add_argument("--timings-out", default="results/timings.csv")
    args = parser.parse_args()

    spark = build_spark()
    df = spark.read.parquet(args.input)

    # --- Warm-up run (not recorded), shared by Query A and Query B ---
    print("Warm-up run (not measured)...")
    try:
        run_with_timeout(lambda: query_a(df).count())
    except Exception as e:
        print(f"  warm-up TIMED OUT or ERRORED ({e}) -- continuing to measured runs")

    # --- Measured runs -- each returns [(run_number, elapsed, status), ...] ---
    results_a = time_query(query_a, df, "Query A (all columns)")
    results_b = time_query(query_b, df, "Query B (4 columns)")

    print("\nQuery A results:", results_a)
    print("Query B results:", results_b)

    # --- Record every measured run into results/timings.csv, per the
    # assignment's requirement that every measured run appear as one row. ---
    input_mb = file_size_mb(args.input)
    rows = []
    for condition, results in [("query_a", results_a), ("query_b", results_b)]:
        for run_number, elapsed, status in results:
            rows.append({
                "system": "spark",
                "task": "task2",
                "condition": condition,
                "dataset_rows": 2_000_000,
                "input_format": "parquet",
                "input_mb": input_mb,
                "run_number": run_number,
                "time_seconds": round(elapsed, 4),
                "status": status,
                "tool_version": TOOL_VERSIONS["spark"],
            })
    append_timing_rows(rows, args.student_id, args.timings_out)
    print(f"Appended {len(rows)} rows to {args.timings_out}")

    # --- Save physical plans for BOTH Query A and Query B, so we can
    # compare ReadSchema / PushedFilters side by side (this is the
    # evidence Task 2 asks for: which columns are read, and whether the
    # filter reaches the file scan). ---
    buf = io.StringIO()
    with redirect_stdout(buf):
        print("========== QUERY A (all columns) ==========")
        query_a(df).explain(mode="formatted")
        print("\n========== QUERY B (4 columns) ==========")
        query_b(df).explain(mode="formatted")
    with open(args.explain_out, "w") as f:
        f.write(buf.getvalue())
    print(f"\nSaved Spark physical plans (Query A and B) to {args.explain_out}")

    spark.stop()


if __name__ == "__main__":
    main()
