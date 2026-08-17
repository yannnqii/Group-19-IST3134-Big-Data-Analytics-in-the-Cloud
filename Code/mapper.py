#!/usr/bin/env python3
"""Map valid BTS flight records to scheduled departure hours for Hadoop Streaming."""

import csv
import sys

CRS_DEP, ARR_DELAY, ARR_DEL15 = 29, 42, 44
CANCELLED, DIVERTED = 47, 49


def main():
    reader = csv.reader(sys.stdin)
    for row in reader:
        if len(row) < 50 or row[0] == "Year":
            continue  # malformed line or header
        try:
            crs_dep = int(row[CRS_DEP])
            cancelled = float(row[CANCELLED])
            diverted = float(row[DIVERTED])
        except ValueError:
            continue  # dirty record -> skip (cleaning happens in the mapper)

        # Cancelled/diverted flights have no arrival delay - skip here;
        # the Spark job keeps them only for the cancellation-rate stats.
        if cancelled == 1.0 or diverted == 1.0:
            continue
        if not (0 <= crs_dep <= 2400):
            continue
        try:
            arr_delay = float(row[ARR_DELAY])
            arr_del15 = int(float(row[ARR_DEL15]))
        except ValueError:
            continue

        hour = (crs_dep // 100) % 24
        # Emit: key TAB value  (Hadoop shuffles/sorts on the key)
        print(f"{hour:02d}\t{arr_delay},{arr_del15}")


if __name__ == "__main__":
    main()
