#!/usr/bin/env python3
import os
import re
import csv
import glob
from collections import defaultdict

METRICS = [
    "cycles", "instructions", "cache-references", "cache-misses",
    "seconds time elapsed", "seconds user", "seconds sys"
]

def extract_metrics_from_file(filepath):
    """Extract performance metrics from a perf.log file."""
    metrics = {}

    with open(filepath, 'r') as f:
        content = f.read()
        # Find the start of the performance stats section
        perf_start = content.rfind("Performance counter stats")
        if perf_start == -1:
            return None
        perf_section = content[perf_start:]

    match = re.search(r'(\d+)\s+cycles:u', perf_section)
    if match:
        metrics['cycles'] = int(match.group(1))

    match = re.search(r'(\d+)\s+instructions:u', perf_section)
    if match:
        metrics['instructions'] = int(match.group(1))

    match = re.search(r'(\d+)\s+cache-references:u', perf_section)
    if match:
        metrics['cache-references'] = int(match.group(1))

    match = re.search(r'(\d+)\s+cache-misses:u', perf_section)
    if match:
        metrics['cache-misses'] = int(match.group(1))

    match = re.search(r'(\d+\.\d+)\s+seconds time elapsed', perf_section)
    if match:
        metrics['seconds_time_elapsed'] = float(match.group(1))

    match = re.search(r'(\d+\.\d+)\s+seconds user', perf_section)
    if match:
        metrics['seconds_user'] = float(match.group(1))

    match = re.search(r'(\d+\.\d+)\s+seconds sys', perf_section)
    if match:
        metrics['seconds_sys'] = float(match.group(1))

    return metrics

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    benchmark_dirs = [d for d in os.listdir(base_dir) if d.startswith('logs_')]

    for benchmark in benchmark_dirs:
        print(f"Processing benchmark: {benchmark}")
        benchmark_path = os.path.join(base_dir, benchmark)

        run_dirs = sorted([d for d in os.listdir(benchmark_path)
                           if d.startswith('run_') and os.path.isdir(os.path.join(benchmark_path, d))],
                          key=lambda x: int(x.split('_')[1]))

        all_run_metrics = []
        for run_dir in run_dirs:
            run_path = os.path.join(benchmark_path, run_dir)
            perf_log_path = os.path.join(run_path, 'perf.log')

            run_num = int(run_dir.split('_')[1])

            if os.path.exists(perf_log_path):
                metrics = extract_metrics_from_file(perf_log_path)
                if metrics:
                    metrics['run'] = run_num
                    all_run_metrics.append(metrics)
                else:
                    print(f"  No metrics found in {perf_log_path}")
            else:
                print(f"  No perf.log found in {run_path}")

        if all_run_metrics:
            csv_path = os.path.join(base_dir, f"{benchmark}_metrics.csv")

            fieldnames = ['run', 'cycles', 'instructions', 'cache-references', 'cache-misses',
                         'seconds_time_elapsed', 'seconds_user', 'seconds_sys']

            with open(csv_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for metrics in all_run_metrics:
                    writer.writerow(metrics)

            print(f"  Wrote metrics to {csv_path}")
        else:
            print(f"  No metrics found for {benchmark}")

if __name__ == "__main__":
    main()
