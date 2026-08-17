#!/usr/bin/env python3
"""Common full-year hourly-delay benchmark implemented with PySpark."""

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        "IST3134-Common-Hourly-Benchmark"
    ).getOrCreate()
    started = time.time()

    raw = (
        spark.read.option("header", True).csv(args.input)
        .select(
            F.col("CRSDepTime").cast("int").alias("crs_dep_time"),
            F.col("ArrDelay").cast("double").alias("arr_delay"),
            F.col("ArrDel15").cast("double").alias("arr_del15"),
            F.col("Cancelled").cast("double").alias("cancelled"),
            F.col("Diverted").cast("double").alias("diverted"),
        )
    )
    raw_count = raw.count()

    valid = (
        raw.filter(
            F.col("crs_dep_time").between(0, 2400)
            & (F.col("cancelled") == 0.0)
            & (F.col("diverted") == 0.0)
            & F.col("arr_delay").isNotNull()
            & F.col("arr_del15").isNotNull()
        )
        .withColumn("hour", (F.col("crs_dep_time") / 100).cast("int") % 24)
        .cache()
    )
    valid_count = valid.count()

    result = (
        valid.groupBy("hour")
        .agg(
            F.count("*").alias("flights"),
            F.round(F.avg("arr_delay"), 2).alias("avg_arr_delay"),
            F.round(F.avg("arr_del15") * 100, 2).alias("pct_delayed15"),
        )
        .orderBy("hour")
    )

    result.coalesce(1).write.mode("overwrite").option("header", True).csv(
        args.output
    )
    result.show(24, truncate=False)
    print(f"Raw records       : {raw_count:,}")
    print(f"Benchmark records : {valid_count:,}")
    print(f"Internal wall time: {time.time() - started:.2f} s")
    spark.stop()


if __name__ == "__main__":
    main()
