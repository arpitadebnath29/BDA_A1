# AI-ASSISTED: A1
"""
scripts/task4_scaling_run.py

Usage (run this yourself, on your Mac):
    python scripts/task4_scaling_run.py --student-id YOUR_ID
"""

import argparse
import subprocess
import statistics
import sys
import time

from machine_info import (
    TIMEOUT_SECONDS, TOOL_VERSIONS, append_timing_rows, file_size_mb,
    measure_repeated, run_with_timeout,
)

DATASETS = {
    100_000: "data/taxi_100k.csv",
    500_000: "data/taxi_500k.csv",
    1_000_000: "data/taxi_1m.csv",
    2_000_000: "data/taxi_2m.csv",
}


def run_mapreduce_once(input_path):
    start = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "mapreduce/task1.py", input_path, "--no-output"],
        capture_output=True, timeout=TIMEOUT_SECONDS,
    )
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode()[-2000:])
    return elapsed


def measure_mapreduce(input_path):
    return measure_repeated(
        measured_fn=lambda: run_mapreduce_once(input_path),
        warmup_fn=lambda: run_mapreduce_once(input_path),
        subprocess_timeout_exc=(subprocess.TimeoutExpired,),
    )


def measure_spark(input_path):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("Task4-Scaling-Spark")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    def aggregation_once():
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        result = (
            df.filter(
                F.col("PULocationID").isNotNull()
                & F.col("fare_amount").isNotNull()
                & (F.col("fare_amount") >= 0)
            )
            .groupBy("PULocationID")
            .agg(F.count(F.lit(1)).alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
        )
        start = time.perf_counter()
        result.count()
        return time.perf_counter() - start

    results = measure_repeated(
        measured_fn=lambda: run_with_timeout(aggregation_once),
        warmup_fn=lambda: run_with_timeout(aggregation_once),
    )
    spark.stop()
    return results


def measure_duckdb(input_path):
    import duckdb

    con = duckdb.connect()
    query = f"""
        SELECT "PULocationID", COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
        FROM read_csv_auto('{input_path}')
        WHERE "PULocationID" IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount >= 0
        GROUP BY "PULocationID"
    """

    def query_once():
        start = time.perf_counter()
        con.execute(query).fetchall()
        return time.perf_counter() - start

    results = measure_repeated(
        measured_fn=lambda: run_with_timeout(query_once),
        warmup_fn=lambda: run_with_timeout(query_once),
    )
    con.close()
    return results


MEASURERS = {
    "mapreduce": measure_mapreduce,
    "spark": measure_spark,
    "duckdb": measure_duckdb,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--output", default="results/timings.csv")
    args = parser.parse_args()

    rows = []

    for system, measure_fn in MEASURERS.items():
        for dataset_rows, input_path in DATASETS.items():
            import os
            if not os.path.exists(input_path):
                print(f"SKIP: {input_path} not found. Run data prep first.")
                continue

            input_mb = file_size_mb(input_path)
            print(f"\n=== {system} | {dataset_rows} rows | {input_path} ===")

            run_results = measure_fn(input_path)

            ok_times = [t for _, t, status in run_results if status == "OK"]
            if ok_times:
                print(f"  median (OK runs only): {statistics.median(ok_times):.4f}s")

            for run_number, elapsed, status in run_results:
                rows.append({
                    "system": system,
                    "task": "task4",
                    "condition": "aggregation",
                    "dataset_rows": dataset_rows,
                    "input_format": "csv",
                    "input_mb": input_mb,
                    "run_number": run_number,
                    "time_seconds": round(elapsed, 4),
                    "status": status,
                    "tool_version": TOOL_VERSIONS[system],
                })

    append_timing_rows(rows, args.student_id, args.output)
    print(f"\nAppended {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
