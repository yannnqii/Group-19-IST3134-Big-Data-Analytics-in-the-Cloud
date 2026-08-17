#!/usr/bin/env python3
"""Run the five full-year flight-delay analyses with PySpark on AWS EMR."""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# The 5 delay-cause columns recorded by BTS (minutes; only filled when a
# flight arrives >= 15 minutes late)
CAUSES = ["CarrierDelay", "WeatherDelay", "NASDelay",
          "SecurityDelay", "LateAircraftDelay"]


def parse_args():
    p = argparse.ArgumentParser(description="Flight delay analysis (PySpark)")
    p.add_argument("--input", required=True, help="Path/glob of BTS CSV files")
    p.add_argument("--carriers", required=True, help="Carrier lookup CSV")
    p.add_argument("--output", required=True, help="Output directory")
    return p.parse_args()


def load_flights(spark, path):
    """Load the raw BTS CSV and cast the ~15 columns we analyse.

    The raw file has 110 columns; selecting early lets Spark drop the
    rest at scan time. Numeric fields arrive as strings like "-5.00".
    """
    df = spark.read.option("header", True).csv(path)
    return df.select(
        F.col("FlightDate"),
        F.col("Month").cast("int").alias("month"),
        F.col("DayOfWeek").cast("int").alias("day_of_week"),
        F.col("Reporting_Airline").alias("carrier"),
        F.col("Origin"), F.col("Dest"),
        F.col("CRSDepTime").cast("int").alias("crs_dep_time"),
        F.col("DepDelay").cast("double").alias("dep_delay"),
        F.col("ArrDelay").cast("double").alias("arr_delay"),
        F.col("ArrDel15").cast("double").alias("arr_del15"),
        F.col("DepDel15").cast("double").alias("dep_del15"),
        F.col("Cancelled").cast("double").alias("cancelled"),
        F.col("Diverted").cast("double").alias("diverted"),
        *[F.col(c).cast("double").alias(c) for c in CAUSES],
    )


def clean(df):
    """Remove records that cannot be analysed.

    Rules (justified in the report):
      - carrier, origin, destination and scheduled dep time must exist
      - scheduled departure time must be a valid hhmm value
    Cancelled/diverted flights are KEPT: they carry no delay values
    (null ArrDelay), but they are needed for the cancellation rate.
    """
    return (
        df.filter(F.col("carrier").isNotNull() & (F.col("carrier") != ""))
          .filter(F.col("Origin").isNotNull() & F.col("Dest").isNotNull())
          .filter(F.col("crs_dep_time").isNotNull()
                  & (F.col("crs_dep_time") >= 0)
                  & (F.col("crs_dep_time") <= 2400))
    )


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("IST3134-Flight-Delays").getOrCreate()
    t0 = time.time()

    # ------------------------------------------------------------------
    # 1. Load
    # ------------------------------------------------------------------
    flights = load_flights(spark, args.input)
    carriers = (
        spark.read.option("header", True).csv(args.carriers)
        .select(F.col("Code").alias("carrier_code"),
                F.col("Description").alias("airline_name"))
    )
    raw_count = flights.count()

    # ------------------------------------------------------------------
    # 2. Clean + enrich
    # ------------------------------------------------------------------
    flights = clean(flights)
    # hhmm -> hour of day (2400 belongs to hour 0)
    flights = flights.withColumn(
        "dep_hour", (F.col("crs_dep_time") / 100).cast("int") % 24)
    flights.cache()  # reused by all five aggregations
    clean_count = flights.count()

    # ------------------------------------------------------------------
    # 3. Aggregations
    # ------------------------------------------------------------------
    # 3.1 Per airline. Broadcast join: the 15-row carrier lookup is sent
    #     to every executor - no shuffle of the big flight table.
    airline = (
        flights.groupBy("carrier")
        .agg(
            F.count("*").alias("flights"),
            F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
            F.round(F.avg("arr_del15") * 100, 2).alias("pct_delayed15"),
            F.round(F.avg("cancelled") * 100, 2).alias("pct_cancelled"),
        )
        .join(F.broadcast(carriers),
              F.col("carrier") == F.col("carrier_code"), "left")
        .select("carrier", "airline_name", "flights", "avg_arr_delay",
                "pct_delayed15", "pct_cancelled")
        .orderBy(F.desc("flights"))
    )

    # 3.2 Top 20 origin airports
    airport = (
        flights.groupBy("Origin")
        .agg(
            F.count("*").alias("flights"),
            F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
            F.round(F.avg("dep_del15") * 100, 2).alias("pct_dep_delayed15"),
            F.round(F.avg("cancelled") * 100, 2).alias("pct_cancelled"),
        )
        .orderBy(F.desc("flights"))
        .limit(20)
    )

    # 3.3 By scheduled departure hour - tests the delay-cascade effect
    hourly = (
        flights.groupBy("dep_hour")
        .agg(
            F.count("*").alias("flights"),
            F.round(F.avg("dep_delay"), 2).alias("avg_dep_delay"),
            F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
            F.round(F.avg("arr_del15") * 100, 2).alias("pct_delayed15"),
        )
        .orderBy("dep_hour")
    )

    # 3.4 Delay causes: one total per cause -> reshape to (cause, minutes)
    sums = flights.agg(
        *[F.sum(c).alias(c) for c in CAUSES]).collect()[0].asDict()
    total = sum(v or 0 for v in sums.values())
    causes = spark.createDataFrame(
        [(c, float(sums[c] or 0),
          round((sums[c] or 0) / total * 100, 2)) for c in CAUSES],
        ["cause", "total_minutes", "share_pct"],
    ).orderBy(F.desc("total_minutes"))

    # 3.5 Monthly trend (one row per month when run on the full year)
    monthly = (
        flights.groupBy("month")
        .agg(
            F.count("*").alias("flights"),
            F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
            F.round(F.avg("arr_del15") * 100, 2).alias("pct_delayed15"),
            F.round(F.avg("cancelled") * 100, 2).alias("pct_cancelled"),
        )
        .orderBy("month")
    )

    # ------------------------------------------------------------------
    # 4. Write results (coalesce(1): outputs are tiny after aggregation)
    # ------------------------------------------------------------------
    for name, df in [("airline_stats", airline), ("airport_stats", airport),
                     ("hourly_delays", hourly), ("delay_causes", causes),
                     ("monthly_trend", monthly)]:
        df.coalesce(1).write.mode("overwrite").option("header", True) \
          .csv(f"{args.output}/{name}")
        print(f"\n=== {name} ===")
        df.show(24, truncate=False)

    elapsed = time.time() - t0
    print(f"\nRaw records      : {raw_count:,}")
    print(f"After cleaning   : {clean_count:,} "
          f"({raw_count - clean_count:,} removed)")
    print(f"Total wall time  : {elapsed:.1f} s")

    spark.stop()


if __name__ == "__main__":
    main()
