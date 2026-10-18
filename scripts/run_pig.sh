#!/usr/bin/env bash
# Run Part 1 (Pig cleaning) in MapReduce mode against HDFS.
# Paths come from config/fraudlens.env, so Pig, Hive and Spark always agree.
# Usage:  run_pig.sh                      whole raw CSV  -> $HDFS_CLEAN_DIR   (default)
#         run_pig.sh <hdfs_in> <hdfs_out> one input file -> one output dir     (used by run_pig_parts.sh)
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

IN_HDFS="${1:-$HDFS_RAW_DIR/$DATASET_FILENAME}"
OUT_HDFS="${2:-$HDFS_CLEAN_DIR}"
RAW_PATH="$(hdfs_uri "$IN_HDFS")"
OUT_PATH="$(hdfs_uri "$OUT_HDFS")"

if ! hdfs_up; then
    echo "ERROR: HDFS is not reachable. Run scripts/start_services.sh first (run_all.sh does this automatically)."
    exit 1
fi

if ! hdfs dfs -test -e "$IN_HDFS" 2>/dev/null; then
    echo "ERROR: Input not in HDFS: $IN_HDFS (run scripts/prepare_data.sh or scripts/split_data.sh)"
    exit 1
fi

if hdfs dfs -test -e "$OUT_HDFS" 2>/dev/null; then
    echo "ERROR: Output already exists: $OUT_HDFS"
    echo "Nothing was deleted. To re-clean intentionally run:  hdfs dfs -rm -r $OUT_HDFS"
    exit 1
fi

# A MapReduce job needs at least one live NodeManager, otherwise it sits in PREP/ACCEPTED forever.
NODES="$(yarn node -list 2>/dev/null | sed -n 's/.*Total Nodes:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1)"
if [[ -z "${NODES:-}" || "$NODES" -lt 1 ]]; then
    echo "ERROR: YARN has no running NodeManager. Run scripts/start_services.sh, then scripts/diagnose.sh."
    exit 1
fi

# Container sizes (MB) for a small VM; heap is about 80 percent of the container.
TASK_MB="${PIG_TASK_MB:-1024}"
HEAP_MB=$(( TASK_MB * 80 / 100 ))
PARALLEL="${PIG_PARALLEL:-2}"

PIG_LOG="$PROJECT/logs/pig_$(date +%Y%m%d_%H%M%S).log"
echo "Pig mode  : mapreduce (explicit)"
echo "Pig input : $RAW_PATH"
echo "Pig output: $OUT_PATH"
echo "Containers: ${TASK_MB} MB (heap ${HEAP_MB} MB), reducers: $PARALLEL"
echo "Pig log   : $PIG_LOG"

# Pig falls back to local mode (and then fails with 'Incomplete HDFS URI, no host') when it cannot
# see the Hadoop client configuration, so put it on the classpath and select the mode explicitly.
export PIG_CLASSPATH="$HADOOP_CONF_DIR${PIG_CLASSPATH:+:$PIG_CLASSPATH}"
export HADOOP_HOME HADOOP_CONF_DIR

# Command and arguments are kept in an array: the executable path and its options must be separate words.
PIG_CMD=(
    "$PIG_HOME/bin/pig"
    -x mapreduce
    -l "$PIG_LOG"
    -param "RAW_PATH=$RAW_PATH"
    -param "OUT_PATH=$OUT_PATH"
    -param "MAP_MB=$TASK_MB"       -param "MAP_HEAP_MB=$HEAP_MB"
    -param "REDUCE_MB=$TASK_MB"    -param "REDUCE_HEAP_MB=$HEAP_MB"
    -param "AM_MB=$TASK_MB"        -param "AM_HEAP_MB=$HEAP_MB"
    -param "PARALLEL=$PARALLEL"
    -f "$PROJECT/pig/clean_paysim.pig"
)
"${PIG_CMD[@]}"

if ! hdfs_job_done "$OUT_HDFS"; then
    echo "ERROR: Pig finished but $OUT_HDFS/_SUCCESS is missing."
    exit 1
fi
echo "Pig cleaning completed: $OUT_HDFS"
