#!/usr/bin/env bash
# FraudLens: detect installed tools and write config/fraudlens.env for this machine.
# Override any location by exporting the variable first, e.g.  HIVE_HOME=/opt/hive bash setup/configure_environment.sh
set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_DIR="$PROJECT/config"
mkdir -p "$CONFIG_DIR"

find_java8() {
    local candidate
    for candidate in \
        "${JAVA_HOME:-}" \
        "$HOME/jdk8" \
        /usr/lib/jvm/java-8-openjdk-amd64 \
        /usr/lib/jvm/java-8-openjdk \
        /usr/lib/jvm/temurin-8-jdk-amd64
    do
        if [[ -n "$candidate" && -x "$candidate/bin/java" ]] &&
           "$candidate/bin/java" -version 2>&1 | grep -q 'version "1\.8\.'; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

# find_tool VARIABLE EXECUTABLE DEFAULT_DIR...
# Uses the directory already in $VARIABLE if it contains bin/EXECUTABLE, otherwise the first default that does.
find_tool() {
    local variable="$1" executable="$2"; shift 2
    local preset="${!variable:-}" candidate
    if [[ -n "$preset" ]]; then
        if [[ -x "$preset/bin/$executable" ]]; then
            printf '%s\n' "$preset"
            return 0
        fi
        echo "WARNING: $variable=$preset has no bin/$executable; trying defaults." >&2
    fi
    for candidate in "$@"; do
        if [[ -x "$candidate/bin/$executable" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    echo "ERROR: Cannot find bin/$executable. Looked in: ${preset:+$preset }$*" >&2
    echo "Install the software or set $variable to its installation directory." >&2
    return 1
}

# Pig/MapReduce container size (MB), chosen from this machine's RAM so jobs fit on small VMs.
MEM_MB="$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 4096)"
NM_MB=$(( MEM_MB * 60 / 100 ))
if   (( NM_MB >= 6000 )); then AUTO_TASK_MB=1536
elif (( NM_MB >= 3500 )); then AUTO_TASK_MB=1024
else                           AUTO_TASK_MB=768
fi

JAVA_PATH="$(find_java8)" || { echo "ERROR: Java 8 was not found. Install Java 8 or set JAVA_HOME."; exit 1; }
HADOOP_PATH="$(find_tool HADOOP_HOME hdfs "$HOME/hadoop" "$HOME/hadoop-3.3.6")" || exit 1
PIG_PATH="$(find_tool PIG_HOME pig "$HOME/pig-0.17.0" "$HOME/pig")" || exit 1
HIVE_PATH="$(find_tool HIVE_HOME beeline "$HOME/hive" "$HOME/apache-hive-4.0.1-bin")" || exit 1
SPARK_PATH="$(find_tool SPARK_HOME spark-submit "$HOME/spark-3.5.9-bin-hadoop3" "$HOME/spark")" || exit 1

cat > "$CONFIG_DIR/fraudlens.env" <<EOFCONFIG
# Generated for this machine by setup/configure_environment.sh - do not commit.
export PROJECT_HOME="$PROJECT"
export JAVA_HOME="$JAVA_PATH"
export HADOOP_HOME="$HADOOP_PATH"
export HADOOP_CONF_DIR="\$HADOOP_HOME/etc/hadoop"
export PIG_HOME="$PIG_PATH"
export HIVE_HOME="$HIVE_PATH"
export SPARK_HOME="$SPARK_PATH"
export PATH="\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PIG_HOME/bin:\$HIVE_HOME/bin:\$SPARK_HOME/bin:\$PATH"
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3.12}
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python3.12}

# HDFS layout
export HDFS_RAW_DIR=/fraudlens/raw
export HDFS_CLEAN_DIR=/fraudlens/processed/paysim_cleaned
export HDFS_MODEL_DIR=/fraudlens/models/spark_fraud_rf_model
export HDFS_PREDICTIONS_DIR=/fraudlens/results/spark_fraud_rf_predictions
export DATASET_FILENAME=PS_20174392719_1491204439457_log.csv

# Spark runtime
export SPARK_MASTER='${SPARK_MASTER:-local[2]}'
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-2g}
export SPARK_SHUFFLE_PARTITIONS=${SPARK_SHUFFLE_PARTITIONS:-8}

# Pig/MapReduce container size in MB (see setup/configure_hadoop.sh for the matching YARN limits)
export PIG_TASK_MB=${PIG_TASK_MB:-$AUTO_TASK_MB}
export PIG_PARALLEL=${PIG_PARALLEL:-2}
# Optional: absolute path of the raw CSV when it is not under data/raw/paysim/
# export DATASET_LOCAL_PATH=/path/to/PS_20174392719_1491204439457_log.csv

# Random forest hyper-parameters (read by spark/fraud_model.py)
export RF_NUM_TREES=${RF_NUM_TREES:-40}
export RF_MAX_DEPTH=${RF_MAX_DEPTH:-8}
export RF_MAX_BINS=${RF_MAX_BINS:-32}
export RF_SUBSAMPLING_RATE=${RF_SUBSAMPLING_RATE:-0.5}
export RF_MIN_INSTANCES_PER_NODE=${RF_MIN_INSTANCES_PER_NODE:-10}

# Hive
export HIVE_JDBC_URL=${HIVE_JDBC_URL:-jdbc:hive2://localhost:10000/default}
EOFCONFIG

echo "Configuration created: $CONFIG_DIR/fraudlens.env"
echo "Detected installations:"
echo "  Java:   $JAVA_PATH"
echo "  Hadoop: $HADOOP_PATH"
echo "  Pig:    $PIG_PATH"
echo "  Hive:   $HIVE_PATH"
echo "  Spark:  $SPARK_PATH"
echo
echo "Next: source \"$CONFIG_DIR/fraudlens.env\" && bash setup/verify_installation.sh"
