# AI-ASSISTED: A1
"""
spark/task3.py
Task 3 (Spark version): join taxi_2m.parquet with taxi_zone_lookup.csv on
PULocationID = LocationID, filter fare_amount >= 0, sum fare_amount per
Borough + Zone, return the top 5 zones by total_fare (ties broken by Zone
ascending).

Usage:
    python spark/task3.py --trips data/taxi_2m.parquet --zones data/taxi_zone_lookup.csv
"""

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from machine_info import TOOL_VERSIONS, append_timing_rows, file_size_mb, measure_repeated, run_with_timeout


def build_spark():
    return (
        SparkSession.builder
        .appName("Task3-JoinTopZones")
        .master("local[*]")
        .getOrCreate()
    )


def build_query(trips, zones):
    filtered_trips = trips.filter(F.col("fare_amount") >= 0)

    joined = filtered_trips.join(
        zones,
        filtered_trips["PULocationID"] == zones["LocationID"],
        "inner",
    )

    grouped = (
        joined.groupBy("Borough", "Zone")
        .agg(F.sum("fare_amount").alias("total_fare"))
    )

    # Tie-break: total_fare descending, then Zone ascending.
    top5 = grouped.orderBy(F.col("total_fare").desc(), F.col("Zone").asc()).limit(5)
    return joined, top5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--student-id", required=True)
    parser.add_argument("--trips", default="data/taxi_2m.parquet")
    parser.add_argument("--zones", default="data/taxi_zone_lookup.csv")
    parser.add_argument("--timings-out", default="results/timings.csv")
    args = parser.parse_args()

    spark = build_spark()

    trips = spark.read.parquet(args.trips)
    zones = spark.read.csv(args.zones, header=True, inferSchema=True)

    def run_once():
        start = time.perf_counter()
        _, top5_run = build_query(trips, zones)
        top5_run.collect()  # forces full materialization of the top-5 result
        return time.perf_counter() - start

    results = measure_repeated(
        measured_fn=lambda: run_with_timeout(run_once),
        warmup_fn=lambda: run_with_timeout(run_once),
    )
    print("\nResults:", results)

    # Show the final result once (not during the timed region). Not saved
    # to a file -- Task 3 doesn't require one (unlike Task 2's explain
    # files), so results/ stays limited to what the assignment actually
    # requires there (timings.csv, scaling_plot.png).
    joined, top5 = build_query(trips, zones)
    top5.show(truncate=False)

    # --- Record every measured run into results/timings.csv ---
    input_mb = file_size_mb(args.trips)
    timing_rows = [
        {
            "system": "spark",
            "task": "task3",
            "condition": "join_top5",
            "dataset_rows": 2_000_000,
            "input_format": "parquet",
            "input_mb": input_mb,
            "run_number": run_number,
            "time_seconds": round(elapsed, 4),
            "status": status,
            "tool_version": TOOL_VERSIONS["spark"],
        }
        for run_number, elapsed, status in results
    ]
    append_timing_rows(timing_rows, args.student_id, args.timings_out)
    print(f"Appended {len(timing_rows)} rows to {args.timings_out}")

    spark.stop()


if __name__ == "__main__":
    main()
