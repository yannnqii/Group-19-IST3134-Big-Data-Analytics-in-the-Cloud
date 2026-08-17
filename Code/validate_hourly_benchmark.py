#!/usr/bin/env python3
"""Validate that Hadoop, Spark, and pandas hourly outputs agree."""

import argparse
import csv
import glob


def read_hadoop(path):
    rows = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            hour, flights, avg_delay, delayed_pct = line.rstrip().split("\t")
            rows[int(hour)] = (int(flights), float(avg_delay), float(delayed_pct))
    return rows


def read_csv(path_or_glob):
    matches = sorted(glob.glob(path_or_glob))
    if len(matches) != 1:
        raise ValueError(f"Expected one CSV for {path_or_glob}, found {matches}")
    rows = {}
    with open(matches[0], newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows[int(row["hour"])] = (
                int(row["flights"]),
                float(row["avg_arr_delay"]),
                float(row["pct_delayed15"]),
            )
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hadoop", required=True)
    parser.add_argument("--spark", required=True)
    parser.add_argument("--pandas", required=True)
    args = parser.parse_args()

    outputs = {
        "Hadoop": read_hadoop(args.hadoop),
        "Spark": read_csv(args.spark),
        "pandas": read_csv(args.pandas),
    }
    reference = outputs["pandas"]
    failures = []
    for method, rows in outputs.items():
        if set(rows) != set(reference):
            failures.append(f"{method}: hour keys differ")
            continue
        for hour, expected in reference.items():
            actual = rows[hour]
            if actual[0] != expected[0] or any(
                abs(a - b) > 0.01 for a, b in zip(actual[1:], expected[1:])
            ):
                failures.append(
                    f"{method} hour {hour:02d}: {actual} != {expected}"
                )

    if failures:
        print("VALIDATION FAILED")
        print("\n".join(failures))
        raise SystemExit(1)
    print("VALIDATION PASSED: Hadoop, Spark, and pandas match for all 24 hours.")


if __name__ == "__main__":
    main()
