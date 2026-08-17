#!/usr/bin/env python3
"""Run the five full-year flight-delay analyses with single-machine pandas."""

import argparse
import glob
import os
import time

import pandas as pd

CAUSES = ["CarrierDelay", "WeatherDelay", "NASDelay",
          "SecurityDelay", "LateAircraftDelay"]

# Load only the columns the analysis needs (15 of 110) - without this,
# a single month already costs ~1.5 GB of RAM.
USECOLS = ["FlightDate", "Month", "DayOfWeek", "Reporting_Airline",
           "Origin", "Dest", "CRSDepTime", "DepDelay", "ArrDelay",
           "ArrDel15", "DepDel15", "Cancelled", "Diverted"] + CAUSES


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Glob of BTS CSV files")
    p.add_argument("--carriers", required=True)
    p.add_argument("--output", required=True)
    args = p.parse_args()
    os.makedirs(args.output, exist_ok=True)

    t0 = time.time()

    # ------------------------------------------------------------------
    # 1. Load - unlike Spark, EVERY row must fit into this machine's RAM
    # ------------------------------------------------------------------
    files = sorted(glob.glob(args.input))
    print(f"Loading {len(files)} file(s)...")
    df = pd.concat(
        (pd.read_csv(f, usecols=USECOLS, low_memory=False) for f in files),
        ignore_index=True)
    carriers = pd.read_csv(args.carriers)
    raw_count = len(df)

    # ------------------------------------------------------------------
    # 2. Clean + enrich (same rules as the Spark job)
    # ------------------------------------------------------------------
    df["CRSDepTime"] = pd.to_numeric(df.CRSDepTime, errors="coerce")
    df = df[
        df.Reporting_Airline.notna() & (df.Reporting_Airline != "")
        & df.Origin.notna() & df.Dest.notna()
        & df.CRSDepTime.notna()
        & (df.CRSDepTime >= 0) & (df.CRSDepTime <= 2400)
    ].copy()
    df["dep_hour"] = (df.CRSDepTime // 100).astype(int) % 24

    # ------------------------------------------------------------------
    # 3. Same five aggregations
    # ------------------------------------------------------------------
    airline = df.groupby("Reporting_Airline").agg(
        flights=("Origin", "size"),
        avg_arr_delay=("ArrDelay", "mean"),
        pct_delayed15=("ArrDel15", "mean"),
        pct_cancelled=("Cancelled", "mean"),
    ).reset_index()
    airline[["pct_delayed15", "pct_cancelled"]] *= 100
    airline = airline.round(2).merge(
        carriers, left_on="Reporting_Airline", right_on="Code", how="left")
    airline = airline[["Reporting_Airline", "Description", "flights",
                       "avg_arr_delay", "pct_delayed15", "pct_cancelled"]] \
        .sort_values("flights", ascending=False)

    airport = df.groupby("Origin").agg(
        flights=("Origin", "size"),
        avg_dep_delay=("DepDelay", "mean"),
        pct_dep_delayed15=("DepDel15", "mean"),
        pct_cancelled=("Cancelled", "mean"),
    ).reset_index()
    airport[["pct_dep_delayed15", "pct_cancelled"]] *= 100
    airport = airport.round(2).sort_values("flights", ascending=False).head(20)

    hourly = df.groupby("dep_hour").agg(
        flights=("Origin", "size"),
        avg_dep_delay=("DepDelay", "mean"),
        avg_arr_delay=("ArrDelay", "mean"),
        pct_delayed15=("ArrDel15", "mean"),
    ).reset_index()
    hourly["pct_delayed15"] *= 100
    hourly = hourly.round(2)

    sums = df[CAUSES].sum()
    causes = pd.DataFrame({
        "cause": CAUSES,
        "total_minutes": sums.values,
        "share_pct": (sums / sums.sum() * 100).round(2).values,
    }).sort_values("total_minutes", ascending=False)

    monthly = df.groupby("Month").agg(
        flights=("Origin", "size"),
        avg_arr_delay=("ArrDelay", "mean"),
        pct_delayed15=("ArrDel15", "mean"),
        pct_cancelled=("Cancelled", "mean"),
    ).reset_index()
    monthly[["pct_delayed15", "pct_cancelled"]] *= 100
    monthly = monthly.round(2)

    for name, res in [("airline_stats", airline), ("airport_stats", airport),
                      ("hourly_delays", hourly), ("delay_causes", causes),
                      ("monthly_trend", monthly)]:
        res.to_csv(f"{args.output}/{name}.csv", index=False)
        print(f"\n=== {name} ===")
        print(res.to_string(index=False))

    print(f"\nRaw records    : {raw_count:,}")
    print(f"After cleaning : {len(df):,}")
    print(f"Total wall time: {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
