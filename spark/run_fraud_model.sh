#!/usr/bin/env bash
# Run Part 3 (Spark MLlib Random Forest). Settings come from config/fraudlens.env.
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

export JAVA_HOME HADOOP_HOME SPARK_HOME PROJECT_HOME
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3.12}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-python3.12}"
# Passed through to fraud_model.py
export HDFS_CLEAN_DIR HDFS_MODEL_DIR HDFS_PREDICTIONS_DIR
export RF_NUM_TREES RF_MAX_DEPTH RF_MAX_BINS RF_SUBSAMPLING_RATE RF_MIN_INSTANCES_PER_NODE

if ! hdfs_job_done "$HDFS_CLEAN_DIR"; then
    echo "ERROR: Cleaned HDFS data missing or incomplete (no _SUCCESS): $HDFS_CLEAN_DIR"
    echo "Run scripts/run_pig.sh first."
    exit 1
fi

for path in "$HDFS_PREDICTIONS_DIR" "$HDFS_MODEL_DIR"; do
    if hdfs dfs -test -e "$path" 2>/dev/null; then
        echo "ERROR: Output already exists: $path"
        echo "Existing results were not deleted. Remove deliberately with: hdfs dfs -rm -r $path"
        exit 1
    fi
done

mkdir -p "$PROJECT/spark/results"
RUN_LOG="$PROJECT/spark/results/spark_run.log"

echo "Spark version:"
"$SPARK_HOME/bin/spark-submit" --version

echo
echo "Starting FraudLens Part 3 (Random Forest)..."
echo "Log: $RUN_LOG"

"$SPARK_HOME/bin/spark-submit" \
    --master "$SPARK_MASTER" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --conf "spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS" \
    "$PROJECT/spark/fraud_model.py" \
    2>&1 | tee "$RUN_LOG"
