# US Flight Delay Analysis at Scale

This IST3134 project analyses 2024 US domestic flight delays using Hadoop
Streaming, Spark on YARN, and pandas.

## Dataset

- Source: [US DOT Bureau of Transportation Statistics](https://www.transtats.bts.gov/Tables.asp?QO_VQ=EFD)
- 12 monthly CSV files, approximately 3.0 GB
- 7,079,061 raw records; 6,965,247 valid benchmark records

Download the raw data with:

```bash
bash code/download_data.sh /tmp/flights
```

## Implementation

| Solution | Code |
|---|---|
| Hadoop | `mapper.py`, `reducer.py` |
| Spark | `spark_hourly_benchmark.py`, `spark_analysis.py` |
| pandas | `pandas_hourly_benchmark.py`, `pandas_baseline.py` |

All three methods applied the same filter-group-aggregate algorithm and produced
matching results for all 24 scheduled departure hours.

| Method | Wall time |
|---|---:|
| Hadoop Streaming | 98.23 s |
| Spark on YARN | 56.55 s |
| pandas | 37.79 s |

## Files

```text
code/       Source code and running scripts
data/       Carrier lookup file
results/    Benchmark results, analysis results, and figures
evidence/   AWS EMR screenshots
```
