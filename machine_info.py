# AI-ASSISTED: A1
"""
scripts/machine_info.py

Every timing script (Task 1, 2, 3, 4, 5) imports from this file instead of
each keeping its own separate copy"""

import csv
import os
import signal

CSV_COLUMNS = [
    "student_id", "system", "task", "condition", "dataset_rows", "input_format",
    "input_mb", "run_number", "time_seconds", "status", "os", "cpu_model",
    "cpu_base_ghz", "cpu_physical_cores", "cpu_logical_processors", "ram_gb",
    "storage_size_gb", "storage_type", "gpu_model", "gpu_clock_mhz",
    "gpu_memory_gb", "python_version", "tool_version",
]

# FILL THESE IN with the real output of `python scripts/collect_machine_info.py`.
# Do not guess -- use "NA" for anything you cannot determine.
MACHINE_INFO = {
    "os": "Darwin 25.5.0 (26.5.2)",
    "cpu_model": "Apple M5",
    "cpu_base_ghz": "NA",
    "cpu_physical_cores": "10",
    "cpu_logical_processors": "10",
    "ram_gb": "16.0",
    "storage_size_gb": "494.3",
    "storage_type": "SSD",
    "gpu_model": "Apple M5",
    "gpu_clock_mhz": "NA",
    "gpu_memory_gb": "NA",
    "python_version": "3.11.16",
}

TOOL_VERSIONS = {
    "mapreduce": "0.7.4",
    "spark": "4.2.0",
    "duckdb": "1.5.5",
}

TIMEOUT_SECONDS = 900


def file_size_mb(path):
    return round(os.path.getsize(path) / (1024 * 1024), 3)


class TimeoutSignal(Exception):
    """Raised when an in-process call (Spark/DuckDB) exceeds TIMEOUT_SECONDS."""
    pass


def _raise_timeout(signum, frame):
    raise TimeoutSignal()


def run_with_timeout(fn, timeout_seconds=TIMEOUT_SECONDS):
    """
    Runs fn() with a wall-clock timeout via SIGALRM (macOS/Linux only).
    Used for in-process Spark/DuckDB calls, which aren't separate
    subprocesses and so can't use subprocess.run(timeout=...). MapReduce
    (mrjob) IS a subprocess per run, so it uses subprocess.run's own
    timeout= instead of this.
    """
    old_handler = signal.signal(signal.SIGALRM, _raise_timeout)
    signal.alarm(timeout_seconds)
    try:
        return fn()
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def measure_repeated(measured_fn, n_runs=3, warmup_fn=None, subprocess_timeout_exc=()):
    """
    Shared "warm-up (optional) + N measured runs" loop, with the
    assignment's timeout rule applied: if any run exceeds 900s, it's
    recorded as (run_number, 900.0, "TIMEOUT_900") and NO further runs in
    this call are attempted -- they're filled in as TIMEOUT_900 without
    re-executing, per "do not repeat a timed-out combination."

    measured_fn() / warmup_fn() should return elapsed seconds, and may
    raise TimeoutSignal (in-process calls, via run_with_timeout) or any
    exception type listed in subprocess_timeout_exc (e.g.
    subprocess.TimeoutExpired, for MapReduce's fresh-process-per-run case).

    Returns a list of (run_number, elapsed_seconds, status) tuples, status
    in {"OK", "TIMEOUT_900", "ERROR"}.
    """
    timeout_exceptions = (TimeoutSignal,) + tuple(subprocess_timeout_exc)

    if warmup_fn is not None:
        print("  warm-up run (not recorded)...")
        try:
            warmup_fn()
        except timeout_exceptions:
            print("  warm-up TIMED OUT")
        except Exception as e:
            print(f"  warm-up ERROR: {e}")

    results = []
    already_timed_out = False
    for run_number in range(1, n_runs + 1):
        if already_timed_out:
            results.append((run_number, 900.0, "TIMEOUT_900"))
            print(f"  run {run_number}/{n_runs}: TIMEOUT_900 (not re-attempted, already timed out)")
            continue
        try:
            elapsed = measured_fn()
            results.append((run_number, elapsed, "OK"))
            print(f"  run {run_number}/{n_runs}: {elapsed:.4f}s")
        except timeout_exceptions:
            results.append((run_number, 900.0, "TIMEOUT_900"))
            print(f"  run {run_number}/{n_runs}: TIMEOUT_900")
            already_timed_out = True
        except Exception as e:
            results.append((run_number, 0.0, "ERROR"))
            print(f"  run {run_number}/{n_runs}: ERROR ({e})")
    return results


def append_timing_rows(rows, student_id, out_path="results/timings.csv"):
    """
    Appends timing rows to results/timings.csv. Each row in `rows` must be
    a dict with at least: system, task, condition, dataset_rows,
    input_format, input_mb, run_number, time_seconds, status, tool_version.
    Machine config and student_id are filled in automatically here so every
    call site doesn't have to repeat them.
    """
    file_exists = os.path.exists(out_path)
    with open(out_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            full_row = {"student_id": student_id, **row, **MACHINE_INFO}
            writer.writerow(full_row)
