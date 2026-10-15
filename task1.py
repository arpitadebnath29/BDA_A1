# AI-ASSISTED: A1
"""
spark/task1.py
Task 1 (Spark): For every non-null PULocationID, compute trip_count
and avg_fare, using only rows where fare_amount IS NOT NULL AND fare_amount >= 0.

Usage:
    python spark/task1.py --input data/taxi_1m.csv --output results/task1_spark_output.csv
"""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    # local[*] uses all available CPU cores on this laptop, in a single
    # process -- this is "Spark local mode", not a real cluster. See
    # README / viva notes for why that distinction matters.
    return (
        SparkSession.builder
        .appName("Task1-TripStats")
        .master("local[*]")
        .getOrCreate()
    )


def run_task1(spark, input_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    result = (
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
        .orderBy("PULocationID")
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/taxi_1m.csv")
    parser.add_argument("--output", default="results/task1_spark_output.csv")
    args = parser.parse_args()

    spark = build_spark()

    start = time.perf_counter()
    result = run_task1(spark, args.input)
    # .count() forces materialization -- without an action, Spark would
    # stay lazy and do nothing (see "lazy evaluation" in the viva notes).
    row_count = result.count()
    elapsed = time.perf_counter() - start

    print(f"Rows in result: {row_count}")
    print(f"Elapsed (single run, not the official benchmark): {elapsed:.4f}s")

    # coalesce(1) so we get a single output CSV file instead of Spark's
    # default multi-part output -- easier to inspect/compare by hand.
    result.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
