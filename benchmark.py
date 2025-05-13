#!/usr/bin/env python3
import subprocess
import sys
import os
import datetime
from pathlib import Path

"""
Benchmarking script: runs 'cargo run --release' using workload.txt multiple times,
collecting only perf logs.
Usage: python benchmark.py <runs>
Example: python benchmark.py 10
"""

# ---- Configuration ----
WORKLOAD_FILE = "workload.txt"
ENTRY_SIZE_BYTES = 8  # 8 bytes key + 8 bytes value
PERF_EVENTS = ["cycles", "instructions", "cache-references", "cache-misses"]
BASE_LOG_DIR = Path("./logs")
BASE_LOG_DIR.mkdir(parents=True, exist_ok=True)


def get_workload_stats(workload_file):
    stats = {}
    with open(workload_file, "r") as f:
        lines = f.readlines()
        stats['puts'] = sum(1 for line in lines if line.startswith('p '))
        stats['gets'] = sum(1 for line in lines if line.startswith('g '))
        stats['deletes'] = sum(1 for line in lines if line.startswith('d '))
        stats['ranges'] = sum(1 for line in lines if line.startswith('r '))
    stats['data_size_bytes'] = stats['puts'] * ENTRY_SIZE_BYTES
    stats['data_size_hr'] = subprocess.check_output([
        'numfmt', '--to=iec', '--suffix=B', str(stats['data_size_bytes'])
    ]).decode().strip()
    return stats


def run_single_benchmark(run_id):
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = BASE_LOG_DIR / f"run_{run_id}_{timestamp}"
    run_dir.mkdir()

    perf_log = run_dir / "perf.log"

    try:
        pid = subprocess.check_output(['pgrep','-x','byron_server']).decode().strip()
        pid_opt = ['-p', pid]
    except subprocess.CalledProcessError:
        pid_opt = []
        print("Warning: byron_server not running; skipping -p.")

    cmd = ['cargo', 'run', '--release']
    with open(perf_log,'w') as perf_out:
        subprocess.run(
            ['perf','stat','-e',','.join(PERF_EVENTS)] + pid_opt + ['--'] + cmd,
            stdout=subprocess.DEVNULL, stderr=perf_out
        )

    return run_dir


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)

    runs = int(sys.argv[1])

    stats = get_workload_stats(WORKLOAD_FILE)
    print("=== Workload Summary ===")
    for k, v in stats.items(): print(f"{k}: {v}")
    print(f"Running 'cargo run --release' for {runs} runs...\n")

    all_runs = []
    for i in range(1, runs+1):
        print(f"--- Run {i}/{runs} ---")
        run_dir = run_single_benchmark(i)
        all_runs.append(run_dir)

    print("\nBenchmarking complete. Perf logs by run directory:")
    for d in all_runs:
        print(f"  {d}")

if __name__ == '__main__':
    main()

