#!/usr/bin/env bash
# Copy the Spark predictions from HDFS to ui/data/predictions.csv so the dashboard can read them.
# (metrics.txt and the Hive log are already local files.) Safe to rerun: only the local copy is replaced.
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

if ! hdfs_up; then
    echo "ERROR: HDFS is not reachable. Run scripts/start_services.sh first."
    exit 1
fi
if ! hdfs_job_done "$HDFS_PREDICTIONS_DIR"; then
    echo "ERROR: Predictions are missing or incomplete: $HDFS_PREDICTIONS_DIR (run spark/run_fraud_model.sh)"
    exit 1
fi

OUT_DIR="$PROJECT/ui/data"
mkdir -p "$OUT_DIR"
TMP="$OUT_DIR/predictions_merged.tmp"

echo "Merging prediction parts from HDFS ..."
hdfs dfs -getmerge "$HDFS_PREDICTIONS_DIR" "$TMP"

# Every part file starts with the CSV header; keep only the first one.
HEADER="$(head -n 1 "$TMP")"
awk -v h="$HEADER" 'NR==1 || $0 != h' "$TMP" > "$OUT_DIR/predictions.csv"
rm -f "$TMP"

ROWS=$(( $(wc -l < "$OUT_DIR/predictions.csv") - 1 ))
echo "Exported $ROWS predictions to $OUT_DIR/predictions.csv"
echo "Header: $HEADER"
echo "Start the dashboard with:  bash scripts/run_ui.sh"
