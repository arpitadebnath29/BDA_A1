# AI-ASSISTED: A1
"""
scripts/task1_verify.py

Run MapReduce, Spark, and DuckDB on taxi_1m.csv and compare their
(PULocationID, trip_count, avg_fare) results and print PASS/FAIL.

Usage:
    python scripts/task1_verify.py
"""

import csv
import io
import subprocess
import sys

import duckdb

INPUT = "data/taxi_1m.csv"
TOLERANCE = 0.01  # allowed difference in avg_fare due to floating point rounding


def get_mapreduce_results():
    result = subprocess.run(
        [sys.executable, "mapreduce/task1.py", INPUT],
        capture_output=True, check=True, text=True,
    )
    data = {}
    for line in result.stdout.splitlines():
        key_str, val_str = line.split("\t")
        pu = key_str.strip('"')
        count, avg = val_str.strip("[]").split(",")
        data[pu] = (int(count), float(avg))
    return data


def get_duckdb_results():
    con = duckdb.connect()
    query = f"""
        SELECT "PULocationID", COUNT(*) AS trip_count, AVG(fare_amount) AS avg_fare
        FROM read_csv_auto('{INPUT}')
        WHERE "PULocationID" IS NOT NULL AND fare_amount IS NOT NULL AND fare_amount >= 0
        GROUP BY "PULocationID"
    """
    rows = con.execute(query).fetchall()
    return {str(int(pu)): (count, avg) for pu, count, avg in rows}


def get_spark_results():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("Verify-Task1").master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv(INPUT, header=True, inferSchema=True)
    result = (
        df.filter(
            F.col("PULocationID").isNotNull()
            & F.col("fare_amount").isNotNull()
            & (F.col("fare_amount") >= 0)
        )
        .groupBy("PULocationID")
        .agg(F.count(F.lit(1)).alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
        .collect()
    )
    spark.stop()
    return {str(row["PULocationID"]): (row["trip_count"], row["avg_fare"]) for row in result}


def main():
    print("Running MapReduce...")
    mr = get_mapreduce_results()
    print(f"  {len(mr)} PULocationID groups")

    print("Running DuckDB...")
    ddb = get_duckdb_results()
    print(f"  {len(ddb)} PULocationID groups")

    print("Running Spark...")
    spark_res = get_spark_results()
    print(f"  {len(spark_res)} PULocationID groups")

    all_keys = set(mr) | set(ddb) | set(spark_res)
    mismatches = []

    for key in sorted(all_keys, key=lambda x: int(x)):
        mr_val = mr.get(key)
        ddb_val = ddb.get(key)
        spark_val = spark_res.get(key)

        if mr_val is None or ddb_val is None or spark_val is None:
            mismatches.append(f"PULocationID {key}: missing from one system "
                               f"(mr={mr_val}, duckdb={ddb_val}, spark={spark_val})")
            continue

        counts = {mr_val[0], ddb_val[0], spark_val[0]}
        if len(counts) != 1:
            mismatches.append(f"PULocationID {key}: trip_count differs "
                               f"(mr={mr_val[0]}, duckdb={ddb_val[0]}, spark={spark_val[0]})")
            continue

        avgs = [mr_val[1], ddb_val[1], spark_val[1]]
        if max(avgs) - min(avgs) > TOLERANCE:
            mismatches.append(f"PULocationID {key}: avg_fare differs beyond tolerance "
                               f"(mr={mr_val[1]}, duckdb={ddb_val[1]}, spark={spark_val[1]})")

    print()
    if not mismatches:
        print(f"PASS: all {len(all_keys)} PULocationID groups match across "
              f"MapReduce, Spark, and DuckDB (within {TOLERANCE} tolerance on avg_fare).")
    else:
        print(f"FAIL: {len(mismatches)} mismatches found:")
        for m in mismatches[:20]:
            print(f"  - {m}")


if __name__ == "__main__":
    main()
