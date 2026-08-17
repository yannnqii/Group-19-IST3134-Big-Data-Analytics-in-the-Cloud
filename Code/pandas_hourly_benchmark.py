#!/usr/bin/env python3
"""Common full-year hourly-delay benchmark implemented with pandas."""

import argparse
import glob
import os
import time

import pandas as pd


USECOLS = ["CRSDepTime", "ArrDelay", "ArrDel15", "Cancelled", "Diverted"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    started = time.time()
    files = sorted(glob.glob(args.input))
    if not files:
        raise FileNotFoundError(f"No input files matched: {args.input}")

    raw = pd.concat(
        (pd.read_csv(path, usecols=USECOLS, low_memory=False) for path in files),
        ignore_index=True,
    )
    raw_count = len(raw)
    for column in USECOLS:
        raw[column] = pd.to_numeric(raw[column], errors="coerce")

    valid = raw[
        raw.CRSDepTime.between(0, 2400)
        & raw.Cancelled.eq(0.0)
        & raw.Diverted.eq(0.0)
        & raw.ArrDelay.notna()
        & raw.ArrDel15.notna()
    ].copy()
    valid["hour"] = (valid.CRSDepTime // 100).astype(int) % 24

    result = valid.groupby("hour").agg(
        flights=("hour", "size"),
        avg_arr_delay=("ArrDelay", "mean"),
        pct_delayed15=("ArrDel15", "mean"),
    ).reset_index()
    result["pct_delayed15"] *= 100
    result = result.round(2).sort_values("hour")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    result.to_csv(args.output, index=False)
    print(result.to_string(index=False))
    print(f"Raw records       : {raw_count:,}")
    print(f"Benchmark records : {len(valid):,}")
    print(f"Internal wall time: {time.time() - started:.2f} s")


if __name__ == "__main__":
    main()
