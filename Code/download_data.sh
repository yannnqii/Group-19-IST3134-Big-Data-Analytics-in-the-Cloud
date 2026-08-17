#!/bin/bash
# ============================================================
# IST3134 - Download US DOT BTS On-Time Performance data (2024)
# Run this ON the EMR primary node.
# 12 monthly ZIPs (~25-30 MB each) -> ~7M flight records, ~3 GB CSV.
# Source: https://www.transtats.bts.gov/Tables.asp?QO_VQ=EFD
# ============================================================
set -e

BASE="https://transtats.bts.gov/PREZIP"
DIR="${1:-/tmp/flights}"
mkdir -p "$DIR"
DIR="$(cd "$DIR" && pwd)"
cd "$DIR"

for m in 1 2 3 4 5 6 7 8 9 10 11 12; do
    f="On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2024_${m}.zip"
    echo "Downloading month $m ..."
    curl -fSL -o "$f" "$BASE/$f"
    unzip -o -q "$f" -x "readme.html"
    # normalise the awkward CSV name (contains spaces/parentheses)
    mv "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_${m}.csv" \
       "ontime_2024_${m}.csv"
    rm "$f"
done

echo "Done:"
ls -lh "$DIR"/ontime_2024_*.csv
