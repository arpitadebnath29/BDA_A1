# AI-ASSISTED: A1
"""
scripts/task4_scaling_plot.py

Task 4 (Scaling): plot the median execution time for each system
across the 4 dataset sizes (100K, 500K, 1M, 2M rows), using the results in results/timings.csv.

Usage:
    python scripts/task4_scaling_plot.py
"""

import csv
import os
import statistics
import sys
from collections import defaultdict

import matplotlib.pyplot as plt

TIMINGS_PATH = "results/timings.csv"
OUTPUT_PATH = "results/scaling_plot.png"


def load_task4_rows(path):
    if not os.path.exists(path):
        sys.exit(f"ERROR: {path} not found. Run scripts/task4_scaling_run.py first.")

    rows = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Include OK and TIMEOUT_900 rows (a timeout is a real outcome
            # we must show). ERROR rows have no meaningful time, so exclude.
            if row["task"] == "task4" and row["status"] in ("OK", "TIMEOUT_900"):
                rows.append(row)

    if not rows:
        sys.exit(
            f"ERROR: no OK/TIMEOUT_900 task4 rows found in {path}. "
            "Run scripts/task4_scaling_run.py to generate real results first."
        )
    return rows


def compute_medians(rows):
    # grouped[system][dataset_rows] = list of (time_seconds, status)
    grouped = defaultdict(lambda: defaultdict(list))
    for row in rows:
        system = row["system"]
        dataset_rows = int(row["dataset_rows"])
        grouped[system][dataset_rows].append((float(row["time_seconds"]), row["status"]))

    # medians[system][dataset_rows] = (median_time, had_timeout)
    medians = defaultdict(dict)
    for system, by_rows in grouped.items():
        for dataset_rows, entries in by_rows.items():
            times = [t for t, _ in entries]
            had_timeout = any(status == "TIMEOUT_900" for _, status in entries)
            medians[system][dataset_rows] = (statistics.median(times), had_timeout)
    return medians


def main():
    rows = load_task4_rows(TIMINGS_PATH)
    medians = compute_medians(rows)

    # Proper display names for the legend, instead of the raw lowercase
    # values stored in the "system" column of timings.csv.
    DISPLAY_NAMES = {
        "mapreduce": "MapReduce (mrjob)",
        "spark": "Spark",
        "duckdb": "DuckDB",
    }

    plt.figure(figsize=(8, 5))
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for i, system in enumerate(sorted(medians.keys())):
        by_rows = medians[system]
        x = sorted(by_rows.keys())
        y = [by_rows[r][0] for r in x]
        color = colors[i % len(colors)]
        display_name = DISPLAY_NAMES.get(system, system)

        plt.plot(x, y, marker="o", label=display_name, color=color)

        # Overlay a red X on any point that includes a timeout, so it
        # reads as "capped at 900s" rather than a genuine fast/slow result.
        timeout_x = [r for r in x if by_rows[r][1]]
        timeout_y = [by_rows[r][0] for r in timeout_x]
        if timeout_x:
            plt.scatter(
                timeout_x, timeout_y, marker="x", s=120, color="red",
                zorder=5, linewidths=2,
                label="_nolegend_",
            )

    # One legend entry explaining the timeout marker, only if any exist.
    any_timeout = any(
        had_timeout for by_rows in medians.values() for (_, had_timeout) in by_rows.values()
    )
    if any_timeout:
        plt.scatter([], [], marker="x", s=120, color="red", linewidths=2,
                    label="TIMEOUT_900 (capped, not a real completion time)")

    # Force x-axis ticks to land exactly on the 4 dataset sizes we actually
    # ran (100K/500K/1M/2M), labeled in plain "100K" style instead of
    # matplotlib's default scientific-notation offset (e.g. "1e6") -- this
    # makes it obvious which point belongs to which of the 4 fixed sizes
    # required by the assignment, rather than an evenly-spaced numeric axis.
    all_dataset_rows = sorted({r for by_rows in medians.values() for r in by_rows.keys()})
    plt.xticks(
        all_dataset_rows,
        [f"{n // 1000}K" if n < 1_000_000 else f"{n // 1_000_000}M" for n in all_dataset_rows],
    )

    plt.xlabel("Dataset rows")
    plt.ylabel("Median execution time (seconds)")
    plt.title("Task 4: Scaling comparison (MapReduce vs Spark vs DuckDB)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=150)
    print(f"Saved plot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
