# AI-ASSISTED: A1
"""
verify_row_counts.py
Check that every generated sample file has exactly the expected number of rows. 
Run this after prepare_data.sql to confirm data prep worked correctly.
"""

import duckdb

EXPECTED = {
    "data/taxi_100k.csv": 100_000,
    "data/taxi_100k.parquet": 100_000,
    "data/taxi_500k.csv": 500_000,
    "data/taxi_500k.parquet": 500_000,
    "data/taxi_1m.csv": 1_000_000,
    "data/taxi_1m.parquet": 1_000_000,
    "data/taxi_2m.csv": 2_000_000,
    "data/taxi_2m.parquet": 2_000_000,
}


def count_rows(con, path):
    if path.endswith(".csv"):
        return con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{path}')").fetchone()[0]
    return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]


def main():
    con = duckdb.connect()
    all_ok = True
    for path, expected in EXPECTED.items():
        try:
            actual = count_rows(con, path)
        except Exception as e:
            print(f"[ERROR] {path}: could not read file ({e})")
            all_ok = False
            continue
        status = "OK" if actual == expected else "MISMATCH"
        if actual != expected:
            all_ok = False
        print(f"[{status}] {path}: expected={expected} actual={actual}")

    print()
    print("ALL CHECKS PASSED" if all_ok else "SOME CHECKS FAILED")


if __name__ == "__main__":
    main()
