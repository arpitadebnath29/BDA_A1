# AI-ASSISTED: A1
"""
spark/task5.py
Task 5 -- Repeated work. Input: taxi_1m.parquet. Aggregation is the same as
Task 1 (trip_count and avg_fare per PULocationID).

Usage:
    python spark/task5.py --input data/taxi_1m.parquet
"""

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from machine_info import TOOL_VERSIONS, TimeoutSignal, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout


def build_spark():
    return (
        SparkSession.builder
        .appName("Task5-RepeatedWork")
        .master("local[*]")
        .getOrCreate()
    )


def aggregation(df):
    return (
        df.filter(
            F.col("PULocationID").isNotNull()
            & F.col("fare_amount").isNotNull()
            & (F.col("fare_amount") >= 0)
        )
        .groupBy("PULocationID")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
        )
    )


def time_runs(df, n_runs=5):
    """5 measured runs, no separate warm-up (Task 5's own spec calls for
    5 runs directly, not the standard warm-up+3 protocol). Each run still
    gets the 900s timeout + timed-out-combination-skip protection from
    machine_info.measure_repeated(). Returns [(run_number, elapsed, status), ...]."""
    def run_once():
        start = time.perf_counter()
        aggregation(df).count()
        return time.perf_counter() - start

    return measure_repeated(measured_fn=lambda: run_with_timeout(run_once), n_runs=n_runs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--input", default="data/taxi_1m.parquet")
    parser.add_argument("--timings-out", default="results/timings.csv")
    args = parser.parse_args()

    spark = build_spark()

    # --- Condition 1: UNCACHED ---
    df_uncached = spark.read.parquet(args.input)
    print("=== Uncached: 5 runs ===")
    uncached_results = time_runs(df_uncached, n_runs=5)

    # --- Condition 2: CACHED ---
    df_cached = spark.read.parquet(args.input)
    df_cached.cache()

    print("\n=== Cache build (count() forces materialization) ===")
    cache_build_status = "OK"
    try:
        start = time.perf_counter()
        row_count = run_with_timeout(df_cached.count)
        cache_build_time = time.perf_counter() - start
        print(f"  cache build time: {cache_build_time:.4f}s ({row_count} rows)")
    except TimeoutSignal:
        cache_build_time = 900.0
        cache_build_status = "TIMEOUT_900"
        print("  cache build TIMED OUT")
    except Exception as e:
        cache_build_time = 0.0
        cache_build_status = "ERROR"
        print(f"  cache build ERROR: {e}")

    print("\n=== Cached: 5 runs ===")
    cached_results = time_runs(df_cached, n_runs=5)

    print("\n--- Summary ---")
    print("Uncached results:", uncached_results)
    print("Cache build time:", cache_build_time, cache_build_status)
    print("Cached results:", cached_results)

    # --- Record every measured run into results/timings.csv ---
    # Per the assignment: "For Task 5 cache construction, use task=task5
    # and condition=spark_cache_build with run_number=0."
    input_mb = file_size_mb(args.input)
    rows = []
    for run_number, elapsed, status in uncached_results:
        rows.append({
            "system": "spark", "task": "task5", "condition": "spark_uncached",
            "dataset_rows": 1_000_000, "input_format": "parquet", "input_mb": input_mb,
            "run_number": run_number, "time_seconds": round(elapsed, 4), "status": status,
            "tool_version": TOOL_VERSIONS["spark"],
        })
    rows.append({
        "system": "spark", "task": "task5", "condition": "spark_cache_build",
        "dataset_rows": 1_000_000, "input_format": "parquet", "input_mb": input_mb,
        "run_number": 0, "time_seconds": round(cache_build_time, 4), "status": cache_build_status,
        "tool_version": TOOL_VERSIONS["spark"],
    })
    for run_number, elapsed, status in cached_results:
        rows.append({
            "system": "spark", "task": "task5", "condition": "spark_cached",
            "dataset_rows": 1_000_000, "input_format": "parquet", "input_mb": input_mb,
            "run_number": run_number, "time_seconds": round(elapsed, 4), "status": status,
            "tool_version": TOOL_VERSIONS["spark"],
        })
    append_timing_rows(rows, args.student_id, args.timings_out)
    print(f"Appended {len(rows)} rows to {args.timings_out}")

    spark.stop()


if __name__ == "__main__":
    main()
