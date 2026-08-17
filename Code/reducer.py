#!/usr/bin/env python3
"""Aggregate Hadoop Streaming mapper output into hourly delay statistics."""

import sys


def emit(key, n, delay_sum, del15_sum):
    if key is not None and n > 0:
        print(f"{key}\t{n}\t{delay_sum / n:.2f}\t{del15_sum / n * 100:.2f}")


def main():
    current_key = None
    n = 0
    delay_sum = 0.0
    del15_sum = 0

    for line in sys.stdin:
        try:
            key, value = line.rstrip("\n").split("\t")
            delay, del15 = value.split(",")
            delay, del15 = float(delay), int(del15)
        except ValueError:
            continue

        if key != current_key:
            emit(current_key, n, delay_sum, del15_sum)  # flush previous key
            current_key, n, delay_sum, del15_sum = key, 0, 0.0, 0

        n += 1
        delay_sum += delay
        del15_sum += del15

    emit(current_key, n, delay_sum, del15_sum)          # flush last key


if __name__ == "__main__":
    main()
