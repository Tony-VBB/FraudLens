#!/usr/bin/env bash
# Master runner: split/upload data -> Pig -> Hive -> Spark. Every stage is skipped when its output is
# already complete (_SUCCESS present) and nothing in HDFS is deleted or overwritten unless you ask (--retrain).
#
# The Pig cleaning works in parts of ROWS_PER_PART rows (default 600000) and offers a stop point after each part:
# you can stop and analyse the parts that are done, or stop and resume later (see scripts/run_pig_parts.sh).
#
# Options:  --max-parts N   clean at most N new parts in this run, then analyse what is done
#           --no-prompt     do not ask after each part
#           --rows N        rows per part (0 = clean the whole file in one go, the old behaviour)
#           --retrain       when new parts were added, delete the old Spark model/predictions and train again
#           --sample-rows N run the whole pipeline on exactly N rows of the CSV (for example 100000), in separate
#                           HDFS folders (suffix _sample<N>) that never touch the full-data results.
#                           SAMPLE_MODE=random (default, seeded, evenly spread) or head (the first N rows).
#                           Without this option the full dataset is used.
set -euo pipefail

while (( $# )); do
    case "$1" in
        --max-parts) export MAX_PARTS="${2:?--max-parts needs a number}"; shift 2 ;;
        --no-prompt) export PART_PROMPT=0; shift ;;
        --rows)      export ROWS_PER_PART="${2:?--rows needs a number}"; shift 2 ;;
        --retrain)   export RETRAIN=1; shift ;;
        --sample-rows) export SAMPLE_ROWS="${2:?--sample-rows needs a number}"; SAMPLE_FLAG=1; shift 2 ;;
        *) echo "Unknown option: $1"; exit 2 ;;
    esac
done

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
# The mode is chosen by this command line only: no --sample-rows (and no SAMPLE_ROWS in the environment) means a full run,
# even if an earlier run was a sample run. The choice is remembered for check_pipeline.sh, export_results.sh and the dashboard.
mkdir -p "$PROJECT_HOME/logs"
if [[ -z "${SAMPLE_ROWS:-}" ]]; then
    SAMPLE_ROWS=0; export SAMPLE_ROWS
fi
load_config
if (( SAMPLE_ROWS > 0 )); then echo "$SAMPLE_ROWS" > "$PROJECT/logs/active_sample_rows"; else rm -f "$PROJECT/logs/active_sample_rows"; fi

LOG="$PROJECT/logs/run_all_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

echo "========== FraudLens Master Runner =========="
echo "Project: $PROJECT"
if (( SAMPLE_ROWS > 0 )); then
    echo "Mode:    SAMPLE of $SAMPLE_ROWS rows ($SAMPLE_MODE, seed $SAMPLE_SEED); HDFS folders end in _sample$SAMPLE_ROWS"
else
    echo "Mode:    full dataset"
fi
echo "Log:     $LOG"
echo

# --- 1. Static checks ------------------------------------------------------
for file in pig/clean_paysim.pig hive/fraud_analysis.hql spark/fraud_model.py \
            spark/run_fraud_model.sh scripts/run_pig.sh scripts/run_hive.sh; do
    if [[ ! -s "$PROJECT/$file" ]]; then
        echo "ERROR: Required pipeline file is missing: $file"
        exit 1
    fi
done

for executable in "$JAVA_HOME/bin/java" "$HADOOP_HOME/bin/hdfs" "$PIG_HOME/bin/pig" \
                  "$HIVE_HOME/bin/beeline" "$SPARK_HOME/bin/spark-submit"; do
    if [[ ! -x "$executable" ]]; then
        echo "ERROR: Required executable not found: $executable"
        exit 1
    fi
done

# Start Hadoop/YARN/Hive automatically when they are down (a fresh machine or a reboot).
ensure_services
if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "ON"; then
    echo "ERROR: HDFS is in safe mode. Free disk space and run: hdfs dfsadmin -safemode leave"
    exit 1
fi

# --- 2./3. Raw data and Pig --------------------------------------------------
PIG_RAN=false
PARTIAL=false
if (( ROWS_PER_PART > 0 )); then
    # Chunked: split into parts, clean part by part, stop whenever you like.
    echo "========== PART 1: PIG (parts of $ROWS_PER_PART rows) =========="
    PARTS_RC=0
    "$PROJECT/scripts/run_pig_parts.sh" || PARTS_RC=$?
    case "$PARTS_RC" in
        0)  ;;
        10) PARTIAL=true ;;
        11) echo
            echo "Stopped. Run the same command again to resume with the remaining parts."
            exit 0 ;;
        *)  exit "$PARTS_RC" ;;
    esac
    [[ "$(cat "$PROJECT/logs/parts_new_count" 2>/dev/null || echo 0)" -gt 0 ]] && PIG_RAN=true
else
    echo "========== STEP 0: RAW DATA =========="
    "$PROJECT/scripts/prepare_data.sh"

    echo
    echo "========== PART 1: PIG =========="
    if hdfs_job_done "$HDFS_CLEAN_DIR"; then
        echo "Cleaned data already complete ($HDFS_CLEAN_DIR/_SUCCESS); skipping Pig."
    elif hdfs dfs -test -e "$HDFS_CLEAN_DIR" 2>/dev/null; then
        echo "ERROR: $HDFS_CLEAN_DIR exists but has no _SUCCESS marker (incomplete earlier run)."
        echo "Inspect it, then remove it deliberately:  hdfs dfs -rm -r $HDFS_CLEAN_DIR"
        exit 1
    else
        "$PROJECT/scripts/run_pig.sh"
        PIG_RAN=true
        rm -f "$HIVE_MARKER"   # Hive results are stale after a new cleaning run
    fi
fi
if [[ "$PARTIAL" == true ]]; then
    echo
    echo ">>> Continuing with the parts that are done: $(grep -E 'parts_(done|total)' "$PROJECT/logs/parts_state.txt" | tr '\n' ' ')"
fi

# --- 4. Hive ---------------------------------------------------------------
echo
echo "========== PART 2: HIVE =========="
if [[ -f "$HIVE_MARKER" ]]; then
    echo "Hive completion marker exists; skipping Hive analysis."
    echo "(Delete logs/hive_analysis_completed to run it again.)"
else
    "$PROJECT/scripts/run_hive.sh"
fi

# --- 5. Spark --------------------------------------------------------------
echo
echo "========== PART 3: SPARK (Random Forest) =========="
MODEL_DONE=false; PRED_DONE=false
hdfs_job_done "$HDFS_MODEL_DIR/metadata" && MODEL_DONE=true
hdfs_job_done "$HDFS_PREDICTIONS_DIR"    && PRED_DONE=true

if [[ "$MODEL_DONE" == true && "$PRED_DONE" == true && "$PIG_RAN" == false ]]; then
    echo "Spark model and predictions already complete; skipping training."
elif [[ "$MODEL_DONE" == true && "$PRED_DONE" == true && "${RETRAIN:-0}" == 1 ]]; then
    echo "New data was added: removing the old model and predictions (--retrain) and training again."
    hdfs dfs -rm -r -skipTrash "$HDFS_MODEL_DIR" "$HDFS_PREDICTIONS_DIR" >/dev/null
    "$PROJECT/spark/run_fraud_model.sh"
elif [[ "$MODEL_DONE" == true && "$PRED_DONE" == true ]]; then
    echo "NOTE: new data was added, but the model/predictions from the previous data exist."
    echo "Train again on the current data with:  bash scripts/run_all.sh --retrain"
    echo "(or remove them yourself:  hdfs dfs -rm -r $HDFS_MODEL_DIR $HDFS_PREDICTIONS_DIR)"
elif hdfs dfs -test -e "$HDFS_MODEL_DIR" 2>/dev/null || hdfs dfs -test -e "$HDFS_PREDICTIONS_DIR" 2>/dev/null; then
    echo "ERROR: Partial Spark output found (model: $MODEL_DONE, predictions: $PRED_DONE)."
    echo "Remove the incomplete output deliberately, then rerun:"
    echo "  hdfs dfs -rm -r $HDFS_MODEL_DIR $HDFS_PREDICTIONS_DIR"
    exit 1
else
    "$PROJECT/spark/run_fraud_model.sh"
fi

echo
echo "FraudLens master run finished. Log: $LOG"
