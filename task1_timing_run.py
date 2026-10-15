# AI-ASSISTED: A1
"""
scripts/task1_timing_run.py

Usage:
    python scripts/task1_timing_run.py --student-id YOUR_ID --input data/taxi_1m.csv
"""

import argparse
import subprocess
import sys
import time

from machine_info import TOOL_VERSIONS, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout


def run_mapreduce_once(input_path):
    start = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "mapreduce/task1.py", input_path, "--no-output"],
        capture_output=True,
    )
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode()[-2000:])
    return elapsed


def time_mapreduce(input_path):
    print("=== MapReduce (mrjob local runner, fresh process per run) ===")
    return measure_repeated(
        measured_fn=lambda: run_mapreduce_once(input_path),
        warmup_fn=lambda: run_mapreduce_once(input_path),
        subprocess_timeout_exc=(subprocess.TimeoutExpired,),
    )


def time_spark(input_path):
    print("\n=== Spark (ONE SparkSession kept alive for warm-up + all runs) ===")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("Task1-Timed-Spark")
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


def time_duckdb(input_path):
    print("\n=== DuckDB (same connection, in-process) ===")
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--input", default="data/taxi_1m.csv")
    parser.add_argument("--output", default="results/timings.csv")
    args = parser.parse_args()

    # results are lists of (run_number, elapsed_seconds, status)
    mr_results = time_mapreduce(args.input)
    spark_results = time_spark(args.input)
    duckdb_results = time_duckdb(args.input)

    print("\n--- Summary (3 measured runtimes each, after warm-up) ---")
    print("MapReduce:", mr_results)
    print("Spark:    ", spark_results)
    print("DuckDB:   ", duckdb_results)

    input_mb = file_size_mb(args.input)
    rows = []
    for system, results in [("mapreduce", mr_results), ("spark", spark_results), ("duckdb", duckdb_results)]:
        for run_number, elapsed, status in results:
            rows.append({
                "system": system,
                "task": "task1",
                "condition": "aggregation",
                "dataset_rows": 1_000_000,
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
