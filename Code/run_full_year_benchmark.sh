#!/bin/bash
# Run the same full-year hourly-delay benchmark with Hadoop, Spark, and pandas.
set -euo pipefail

CODE_SOURCE="${1:?Usage: run_full_year_benchmark.sh local-or-s3-code-path s3://bucket/path/results}"
RESULT_S3="${2:?Usage: run_full_year_benchmark.sh local-or-s3-code-path s3://bucket/path/results}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "$CODE_SOURCE" == "local" ]]; then
  ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
else
  ROOT=/home/hadoop/flight-delays-benchmark
fi
DATA=/tmp/flights
RESULTS=/tmp/ist3134-full-year-results
HDFS_ROOT=/user/hadoop/benchmark

mkdir -p "$ROOT" "$RESULTS"
if [[ "$CODE_SOURCE" != "local" ]]; then
  aws s3 cp "$CODE_SOURCE/" "$ROOT/" --recursive
fi
chmod +x "$ROOT"/code/*.py "$ROOT"/code/*.sh

upload_results() {
  aws s3 cp "$RESULTS/" "$RESULT_S3/" --recursive || true
}
trap upload_results EXIT

if ! compgen -G "$DATA/ontime_2024_*.csv" >/dev/null; then
  "$ROOT/code/download_data.sh" "$DATA" >"$RESULTS/download_2024.log" 2>&1
fi

hadoop fs -rm -r -f "$HDFS_ROOT" >/dev/null 2>&1 || true
hadoop fs -mkdir -p "$HDFS_ROOT/input"
hadoop fs -put "$DATA"/ontime_2024_*.csv "$HDFS_ROOT/input/"
hadoop fs -ls -h "$HDFS_ROOT/input" >"$RESULTS/hdfs_input_listing.txt"

/usr/bin/time -v -o "$RESULTS/hadoop_full_year.time" \
  hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
  -D mapreduce.job.reduces=3 \
  -files "$ROOT/code/mapper.py,$ROOT/code/reducer.py" \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input "$HDFS_ROOT/input/ontime_2024_*.csv" \
  -output "$HDFS_ROOT/results_hadoop" \
  >"$RESULTS/hadoop_full_year.log" 2>&1
hadoop fs -cat "$HDFS_ROOT/results_hadoop/part-*" | sort -n \
  >"$RESULTS/hadoop_full_year_hourly.tsv"

/usr/bin/time -v -o "$RESULTS/spark_full_year_common.time" \
  spark-submit --deploy-mode client \
  "$ROOT/code/spark_hourly_benchmark.py" \
  --input "hdfs://$HDFS_ROOT/input/ontime_2024_*.csv" \
  --output "hdfs://$HDFS_ROOT/results_spark" \
  >"$RESULTS/spark_full_year_common.log" 2>&1
hadoop fs -get "$HDFS_ROOT/results_spark" "$RESULTS/spark_output"

python3 -m pip install --user pandas >"$RESULTS/pandas_install.log" 2>&1 || true
/usr/bin/time -v -o "$RESULTS/pandas_full_year_common.time" \
  python3 "$ROOT/code/pandas_hourly_benchmark.py" \
  --input "$DATA/ontime_2024_*.csv" \
  --output "$RESULTS/pandas_full_year_hourly.csv" \
  >"$RESULTS/pandas_full_year_common.log" 2>&1

python3 "$ROOT/code/validate_hourly_benchmark.py" \
  --hadoop "$RESULTS/hadoop_full_year_hourly.tsv" \
  --spark "$RESULTS/spark_output/part-*.csv" \
  --pandas "$RESULTS/pandas_full_year_hourly.csv" \
  | tee "$RESULTS/validation.log"

upload_results
trap - EXIT
echo "Benchmark complete. Results uploaded to $RESULT_S3/"
