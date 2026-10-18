#!/usr/bin/env bash
# Part 1 (Pig cleaning) in chunks of ROWS_PER_PART rows (default 600000), with a stop point after every part.
#
#   raw part  $HDFS_RAW_PARTS_DIR/part_0003.csv
#     -> Pig  -> $HDFS_PARTS_DIR/part_0003/            (staging, _SUCCESS = this part is finished)
#     -> files moved into $HDFS_CLEAN_DIR/p0003-part-r-*   (the folder Hive and Spark read)
#
# After each part you can continue, stop and analyse what is done, or stop and resume later:
#   * at the prompt:        Enter = continue | a = stop and analyse what is done | q = stop, resume later
#                           (the prompt continues by itself after PART_PROMPT_TIMEOUT seconds, default 20)
#   * from another shell:   touch logs/STOP   -> stop after the current part and analyse what is done
#   * Ctrl-C at any time:   the part being cleaned is discarded; finished parts are kept
#   * MAX_PARTS=N / --max-parts N: clean at most N new parts, then analyse what is done
# Rerun the same command to resume: finished parts are skipped.
#
# Exit status:  0 all parts finished   10 stopped, analyse what is done   11 stopped, nothing more to do now
# Options:  --max-parts N   --no-prompt   (or the environment variables MAX_PARTS, PART_PROMPT=0)
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

MAX_PARTS="${MAX_PARTS:-0}"
PART_PROMPT="${PART_PROMPT:-1}"
PART_PROMPT_TIMEOUT="${PART_PROMPT_TIMEOUT:-20}"
while (( $# )); do
    case "$1" in
        --max-parts) MAX_PARTS="${2:?--max-parts needs a number}"; shift 2 ;;
        --no-prompt) PART_PROMPT=0; shift ;;
        *) echo "Unknown option: $1"; exit 2 ;;
    esac
done

STATE_FILE="$PROJECT/logs/parts_state.txt"
NEW_FILE="$PROJECT/logs/parts_new_count"
STOP_FILE="$PROJECT/logs/STOP"
echo 0 > "$NEW_FILE"

# Data from an earlier single-file run is already complete: nothing to chunk.
if hdfs_job_done "$HDFS_CLEAN_DIR" && ! hdfs dfs -test -e "$HDFS_PARTS_DIR" 2>/dev/null; then
    echo "Cleaned data from an earlier full run exists ($HDFS_CLEAN_DIR); not cleaning in parts."
    exit 0
fi

bash "$PROJECT/scripts/split_data.sh"
mapfile -t PARTS < <(list_raw_parts)
TOTAL="${#PARTS[@]}"
(( TOTAL > 0 )) || { echo "ERROR: no raw parts found in $HDFS_RAW_PARTS_DIR"; exit 1; }

# Results computed from the files in the clean folder are out of date as soon as the folder changes.
invalidate_results() {
    hdfs dfs -rm -f "$HDFS_CLEAN_DIR/_SUCCESS" >/dev/null 2>&1 || true
    rm -f "$HIVE_MARKER"
}

# Move a finished part's output into the clean folder. Safe to repeat: it only moves what is still in staging.
publish_part() {
    local name="$1" number="${1#part_}" file
    local files
    files="$(hdfs dfs -ls "$HDFS_PARTS_DIR/$name" 2>/dev/null | awk '{print $NF}' | grep '/part-' || true)"
    [[ -n "$files" ]] || return 0
    invalidate_results
    hdfs dfs -mkdir -p "$HDFS_CLEAN_DIR"
    while IFS= read -r file; do
        hdfs dfs -mv "$file" "$HDFS_CLEAN_DIR/p${number}-$(basename "$file")"
    done <<< "$files"
}

part_published() {   # true when at least one file of this part is in the clean folder
    local listing
    listing="$(hdfs dfs -ls "$HDFS_CLEAN_DIR" 2>/dev/null || true)"   # no "grep -q" on a pipe: SIGPIPE + pipefail
    [[ "$listing" == *"/p${1#part_}-part-"* ]]
}

DECISION=c
ask_after_part() {   # sets DECISION to c (continue), a (analyse what is done) or q (quit, resume later)
    local done_now="$1" new_now="$2" answer=""
    DECISION=c
    if [[ -f "$STOP_FILE" ]]; then
        rm -f "$STOP_FILE"
        echo "logs/STOP found: stopping after this part."
        DECISION=a; return
    fi
    if (( MAX_PARTS > 0 && new_now >= MAX_PARTS )); then
        echo "Reached --max-parts $MAX_PARTS: stopping after this part."
        DECISION=a; return
    fi
    [[ "$PART_PROMPT" != 0 && -t 0 ]] || return 0
    echo
    echo ">>> $done_now of $TOTAL parts are done."
    echo ">>> [Enter] continue   [a] stop and analyse what is done   [q] stop, resume later"
    if (( PART_PROMPT_TIMEOUT > 0 )); then
        echo ">>> (continuing automatically in ${PART_PROMPT_TIMEOUT}s)"
        read -r -t "$PART_PROMPT_TIMEOUT" answer || answer=""
    else
        read -r answer || answer=""
    fi
    case "${answer,,}" in
        a*) DECISION=a ;;
        q*) DECISION=q ;;
    esac
}

# Record how much of the data the results are based on (the dashboard shows this). Updated after every part.
write_state() {
    {
        echo "parts_done=$DONE"
        echo "parts_total=$TOTAL"
        echo "rows_per_part=$ROWS_PER_PART"
        echo "updated=$(date '+%Y-%m-%d %H:%M')"
    } > "$STATE_FILE"
}

NEW=0
DONE=0
STOPPED=""
for i in "${!PARTS[@]}"; do
    name="${PARTS[$i]}"
    stage="$HDFS_PARTS_DIR/$name"
    echo
    echo "========== PART $((i + 1)) of $TOTAL: $name =========="

    if hdfs_job_done "$stage"; then
        publish_part "$name"          # finishes a move that an earlier interruption cut short
        part_published "$name" && echo "Already cleaned; skipping." || echo "Already cleaned (it produced no valid rows)."
        DONE=$((DONE + 1)); write_state
        continue
    fi
    if hdfs dfs -test -e "$stage" 2>/dev/null; then
        echo "Discarding the unfinished output of an interrupted run: $stage"
        hdfs dfs -rm -r -f -skipTrash "$stage" >/dev/null
    fi

    # </dev/null keeps Hadoop/Pig from swallowing keys typed while the part is running.
    bash "$PROJECT/scripts/run_pig.sh" "$HDFS_RAW_PARTS_DIR/$name.csv" "$stage" </dev/null
    publish_part "$name"
    DONE=$((DONE + 1)); write_state
    NEW=$((NEW + 1))
    echo "$NEW" > "$NEW_FILE"
    echo "Part $name added to $HDFS_CLEAN_DIR"

    if (( DONE < TOTAL )); then
        ask_after_part "$DONE" "$NEW"
        if [[ "$DECISION" != c ]]; then STOPPED="$DECISION"; break; fi
    fi
done

write_state
if [[ "$STOPPED" == q ]]; then
    echo
    echo "Stopped after $DONE of $TOTAL parts. Nothing was analysed. Run the same command again to resume."
    exit 11
fi

if (( DONE == 0 )); then
    echo "No part has been cleaned yet."
    exit 11
fi

# Mark the clean folder complete so Hive and Spark accept it (also when only some parts are in it).
hdfs dfs -test -e "$HDFS_CLEAN_DIR/_SUCCESS" 2>/dev/null || hdfs dfs -touchz "$HDFS_CLEAN_DIR/_SUCCESS"

echo
if (( DONE == TOTAL )); then
    echo "All $TOTAL parts are cleaned and in $HDFS_CLEAN_DIR"
    exit 0
fi
echo "Stopped after $DONE of $TOTAL parts: continuing with the data that is done."
echo "Run the same command again later to clean the remaining $((TOTAL - DONE)) parts."
exit 10
