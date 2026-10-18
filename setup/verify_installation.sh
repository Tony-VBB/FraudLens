#!/usr/bin/env bash
# FraudLens: verify that every configured executable and Python dependency is usable.
set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$PROJECT/config/fraudlens.env"

if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Configuration not found: $CONFIG"
    echo "Run setup/configure_environment.sh first."
    exit 1
fi

source "$CONFIG"

echo "========== FraudLens Installation Verification =========="

FAILURES=0

check_command() {
    local name="$1" command_path="$2"
    if [[ -x "$command_path" ]]; then
        printf 'OK: %s -> %s\n' "$name" "$command_path"
    else
        printf 'MISSING: %s -> %s\n' "$name" "$command_path"
        FAILURES=$((FAILURES + 1))
    fi
}

check_command "Java"         "$JAVA_HOME/bin/java"
check_command "Hadoop"       "$HADOOP_HOME/bin/hadoop"
check_command "HDFS"         "$HADOOP_HOME/bin/hdfs"
check_command "Pig"          "$PIG_HOME/bin/pig"
check_command "Hive Beeline" "$HIVE_HOME/bin/beeline"
check_command "Spark"        "$SPARK_HOME/bin/spark-submit"

echo
echo "========== Java Version =========="
"$JAVA_HOME/bin/java" -version 2>&1 || FAILURES=$((FAILURES + 1))
if ! "$JAVA_HOME/bin/java" -version 2>&1 | grep -q 'version "1\.8\.'; then
    echo "ERROR: JAVA_HOME is not Java 8."
    FAILURES=$((FAILURES + 1))
fi

echo
echo "========== Python Dependencies (${PYSPARK_PYTHON}) =========="
# setuptools provides the distutils shim that PySpark 3.5 needs on Python 3.12.
if "$PYSPARK_PYTHON" -c \
    'import numpy, packaging, setuptools; print("NumPy:", numpy.__version__); print("packaging:", packaging.__version__); print("setuptools:", setuptools.__version__)'
then
    :
else
    echo "ERROR: Python dependency check failed (need numpy, packaging, setuptools)."
    FAILURES=$((FAILURES + 1))
fi

echo
echo "========== Spark Version =========="
if "$SPARK_HOME/bin/spark-submit" --version 2>&1; then
    :
else
    echo "ERROR: Spark version check failed."
    FAILURES=$((FAILURES + 1))
fi

echo
echo "========== HDFS (informational) =========="
if hdfs dfs -test -e / 2>/dev/null; then
    echo "HDFS reachable. $(hdfs dfsadmin -safemode get 2>/dev/null || true)"
else
    echo "HDFS is not reachable yet. Run scripts/start_services.sh before the pipeline."
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "RESULT: All required executable and Python checks passed."
else
    echo "RESULT: $FAILURES check(s) failed. Resolve these before running the pipeline."
    exit 1
fi
