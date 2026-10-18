#!/usr/bin/env bash
# Run Part 2 (Hive analysis) through Beeline and capture the output.
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

if ! hdfs_job_done "$HDFS_CLEAN_DIR"; then
    echo "ERROR: Cleaned data missing or incomplete: $HDFS_CLEAN_DIR (run scripts/run_pig.sh)"
    exit 1
fi

if ! timeout 5 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
    echo "ERROR: HiveServer2 is not listening on localhost:10000 (run scripts/start_services.sh)."
    exit 1
fi

"$HIVE_HOME/bin/beeline" \
    -u "$HIVE_JDBC_URL" \
    --hivevar "HDFS_CLEAN_DIR=$HDFS_CLEAN_DIR" \
    -f "$PROJECT/hive/fraud_analysis.hql" \
    2>&1 | tee "$PROJECT/hive/fraud_analysis_output.log"

touch "$HIVE_MARKER"
