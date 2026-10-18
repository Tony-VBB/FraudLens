#!/usr/bin/env bash
# Upload the raw PaySim CSV to HDFS (never overwrites an existing upload).
# The CSV is searched for in data/raw/paysim/, data/raw/ and data/ (or point DATASET_LOCAL_PATH at it).
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

HDFS_CSV="$HDFS_RAW_DIR/$DATASET_FILENAME"

if hdfs dfs -test -e "$HDFS_CSV" 2>/dev/null; then
    echo "HDFS dataset already exists; preserving it: $HDFS_CSV"
    exit 0
fi

if ! LOCAL_CSV="$(find_local_dataset)"; then
    echo "ERROR: $DATASET_FILENAME was not found."
    echo "Looked in: data/raw/paysim/, data/raw/, data/ (and DATASET_LOCAL_PATH)."
    echo "Download PaySim from https://www.kaggle.com/datasets/ealaxi/paysim1 and place the CSV in:"
    echo "    $PROJECT/data/raw/paysim/"
    echo "A differently named file? Set DATASET_FILENAME in config/fraudlens.env."
    exit 1
fi

# Fail early on a wrong or truncated file instead of in the middle of a MapReduce job.
HEADER="$(head -n 1 "$LOCAL_CSV" | tr -d '\r')"
if [[ "$HEADER" != step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud ]]; then
    echo "ERROR: Unexpected CSV header in $LOCAL_CSV"
    echo "Found:    $HEADER"
    echo "Expected: step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud"
    exit 1
fi

echo "Using local dataset: $LOCAL_CSV"
echo "Creating HDFS raw-data directory..."
hdfs dfs -mkdir -p "$HDFS_RAW_DIR"
echo "Uploading dataset (about 470 MB)..."
hdfs dfs -put "$LOCAL_CSV" "$HDFS_CSV"
echo "Data preparation completed: $HDFS_CSV"
