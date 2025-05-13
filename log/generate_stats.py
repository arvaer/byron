import os
import pandas as pd
import numpy as np
import re
from pathlib import Path

"""
Improved performance analysis script:
- Formats numbers with thousands separators and two decimal places.
- Prints per-size mean and std, plus summary (mean of means, range of means, std of means).
- Displays cache-miss metric as percentage of cache references.
- Supports numeric sizes (e.g., 10k, 1m) and non-numeric keys (e.g., gaussian, uniform).
Usage: Place in the same directory as *_metrics.csv and run: `python analyze.py`
"""

METRICS_CSV_PATTERN = r'logs_([\w-]+)_([\w-]+)_metrics\.csv'
BASE_DIR = Path(__file__).parent

METRICS = [
    'cycles', 'instructions', 'cache-references', 'cache-misses',
    'seconds_time_elapsed', 'seconds_user', 'seconds_sys'
]


def load_csv_data(base_dir):
    """Load all CSV metrics files and organize by benchmark key."""
    data_map = {}
    for fname in os.listdir(base_dir):
        if fname.endswith('_metrics.csv'):
            m = re.match(METRICS_CSV_PATTERN, fname)
            if not m:
                continue
            op, size = m.groups()
            key = f"{op}_{size}"
            df = pd.read_csv(base_dir / fname)
            data_map[key] = df
            print(f"Loaded {fname}: {len(df)} runs")
    return data_map


def organize_by_operation(data_map):
    """Organize data by operation type (e.g., 'get', 'put', 'range')."""
    ops = {}
    for key, df in data_map.items():
        op, subtype = key.split('_', 1)
        ops.setdefault(op, {})[subtype] = df
    return ops


def size_order(s):
    """Sort numeric sizes first, then non-numeric alphabetically."""
    m = re.match(r'^(?P<num>\d+(?:\.\d+)?)(?P<suffix>[kKmM]?)$', s)
    if m:
        num = float(m.group('num'))
        suffix = m.group('suffix').lower()
        factor = 1
        if suffix == 'k': factor = 1e3
        elif suffix == 'm': factor = 1e6
        return (0, num * factor)
    return (1, s)


def simple_stats(ops_data):
    """Print formatted stats and summaries per operation and metric."""
    for op, variants in ops_data.items():
        print(f"\n=== Operation: {op.upper()} ===")
        for metric in METRICS:
            print(f"-- Metric: {metric} --")
            means = []

            # sort both numeric and non-numeric keys
            sorted_keys = sorted(variants.keys(), key=size_order)

            for key in sorted_keys:
                df = variants[key]
                if metric not in df.columns and metric != 'cache-misses':
                    continue

                if metric == 'cache-misses':
                    if 'cache-references' not in df.columns:
                        continue
                    series = df['cache-misses'] / df['cache-references'] * 100
                    label = 'cache-miss %'
                else:
                    series = df[metric]
                    label = metric

                mean = series.mean()
                std = series.std()
                means.append(mean)
                key_label = key.upper()
                if metric == 'cache-misses':
                    print(f"{key_label:>12}% : {mean:,.2f} ± {std:,.2f}%")
                else:
                    print(f"{key_label:>12} : {mean:,.2f} ± {std:,.2f}")


            if means:
                overall_mean = np.mean(means)
                overall_std = np.std(means, ddof=1)
                overall_range = np.max(means) - np.min(means)
                if metric == 'cache-misses':
                    print(
                        f"Summary: mean of means = {overall_mean:,.2f}%, "
                        f"range = {overall_range:,.2f}%, "
                        f"std of means = {overall_std:,.2f}%\n"
                    )
                else:
                    print(
                        f"Summary: mean of means = {overall_mean:,.2f}, "
                        f"range = {overall_range:,.2f}, "
                        f"std of means = {overall_std:,.2f}\n"
                    )


def main():
    data_map = load_csv_data(BASE_DIR)
    ops_data = organize_by_operation(data_map)
    simple_stats(ops_data)

if __name__ == '__main__':
    main()

