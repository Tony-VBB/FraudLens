#!/usr/bin/env bash
# Split the raw PaySim CSV into parts of ROWS_PER_PART data rows (default 600000) and store them in HDFS
# as $HDFS_RAW_PARTS_DIR/part_0001.csv, part_0002.csv, ... (no header line; Pig does not need one).
# The CSV is streamed straight into HDFS: no second copy is written to the local disk.
# Safe to rerun: when the split already finished it does nothing.
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

if (( ROWS_PER_PART < 1 )); then
    echo "ERROR: ROWS_PER_PART must be a positive number (got '$ROWS_PER_PART')."
    exit 1
fi

DONE_MARK="$HDFS_RAW_PARTS_DIR/_SPLIT_DONE"
if hdfs dfs -test -e "$DONE_MARK" 2>/dev/null; then
    echo "Raw parts already in HDFS ($(list_raw_parts | wc -l) parts in $HDFS_RAW_PARTS_DIR); nothing to split."
    exit 0
fi

# Source: the local CSV when it exists, otherwise the full file already uploaded to HDFS.
if LOCAL_CSV="$(find_local_dataset)"; then
    SOURCE_DESC="local file $LOCAL_CSV"
    emit_csv() { cat "$LOCAL_CSV"; }
elif hdfs dfs -test -e "$HDFS_RAW_DIR/$DATASET_FILENAME" 2>/dev/null; then
    SOURCE_DESC="HDFS file $HDFS_RAW_DIR/$DATASET_FILENAME"
    emit_csv() { hdfs dfs -cat "$HDFS_RAW_DIR/$DATASET_FILENAME"; }
else
    echo "ERROR: $DATASET_FILENAME was not found (looked in data/raw/paysim/, data/raw/, data/ and in HDFS)."
    echo "Download PaySim from https://www.kaggle.com/datasets/ealaxi/paysim1 and place the CSV in:"
    echo "    $PROJECT/data/raw/paysim/"
    exit 1
fi

# Fail early on a wrong or truncated file.
HEADER="$(emit_csv | head -n 1 | tr -d '\r' || true)"
EXPECTED="step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud"
if [[ "$HEADER" != "$EXPECTED" ]]; then
    echo "ERROR: Unexpected CSV header in the $SOURCE_DESC"
    echo "Found:    $HEADER"
    echo "Expected: $EXPECTED"
    exit 1
fi

# An earlier split that was interrupted left no _SPLIT_DONE marker. Those files are only a copy of the
# CSV, so they are discarded and the split starts again.
if hdfs dfs -test -e "$HDFS_RAW_PARTS_DIR" 2>/dev/null; then
    echo "Found an unfinished split in $HDFS_RAW_PARTS_DIR; discarding it and splitting again."
    hdfs dfs -rm -r -f -skipTrash "$HDFS_RAW_PARTS_DIR" >/dev/null
fi
hdfs dfs -mkdir -p "$HDFS_RAW_PARTS_DIR"

# Sample mode (--sample-rows N): keep exactly N data rows. "random" = seeded selection sampling (Knuth's algorithm S):
# every row has the same chance, the original order (the time order of PaySim) is kept, and the same seed always gives
# the same rows. "head" = the first N rows (only the first simulated hours, so very few frauds).
sample_filter() { cat; }
if (( SAMPLE_ROWS > 0 )); then
    TOTAL_ROWS="$(emit_csv | tr -d '\r' | tail -n +2 | wc -l)"
    if (( SAMPLE_ROWS >= TOTAL_ROWS )); then
        echo "NOTE: the file has only $TOTAL_ROWS data rows; --sample-rows $SAMPLE_ROWS keeps all of them."
    else
        echo "Sample mode: keeping $SAMPLE_ROWS of $TOTAL_ROWS rows ($SAMPLE_MODE, seed $SAMPLE_SEED)."
        case "$SAMPLE_MODE" in
            random) sample_filter() { awk -v n="$SAMPLE_ROWS" -v t="$TOTAL_ROWS" -v seed="$SAMPLE_SEED" \
                        'BEGIN { srand(seed); need = n; left = t } { if (need > 0 && rand() * left < need) { print; need-- } left-- }'; } ;;
            head)   sample_filter() { awk -v n="$SAMPLE_ROWS" 'NR <= n'; } ;;
            *) echo "ERROR: SAMPLE_MODE must be random or head (got '$SAMPLE_MODE')."; exit 1 ;;
        esac
    fi
fi

echo "Splitting the $SOURCE_DESC into parts of $ROWS_PER_PART rows ..."
export HDFS_RAW_PARTS_DIR
emit_csv | tr -d '\r' | tail -n +2 | sample_filter \
    | split -l "$ROWS_PER_PART" --numeric-suffixes=1 -a 4 \
        --filter='hdfs dfs -put -f - "$HDFS_RAW_PARTS_DIR/$FILE.csv"' - part_

PARTS="$(list_raw_parts | wc -l)"
if (( PARTS < 1 )); then
    echo "ERROR: The split produced no parts."
    exit 1
fi
hdfs dfs -touchz "$DONE_MARK"
echo "Split finished: $PARTS parts in $HDFS_RAW_PARTS_DIR"
