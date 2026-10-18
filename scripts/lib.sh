# Shared helpers, sourced by the other scripts. Not meant to be executed directly.

# Repository root = parent of the directory holding the calling script.
fraudlens_root() {
    cd "$(dirname "${BASH_SOURCE[1]}")/.." && pwd
}

# Load config/fraudlens.env. The repository root is taken from the caller's PROJECT_HOME
# (set from the script's own location), never from a path baked into the generated config,
# so the repository can be moved or cloned anywhere. A missing config is generated
# automatically (this is what makes a fresh `git clone` runnable).
load_config() {
    local root
    root="${PROJECT_HOME:-$(fraudlens_root)}"
    CONFIG="$root/config/fraudlens.env"
    if [[ ! -f "$CONFIG" ]]; then
        echo "config/fraudlens.env not found - generating it with setup/configure_environment.sh ..."
        if ! bash "$root/setup/configure_environment.sh"; then
            echo "ERROR: Could not generate the configuration."
            echo "Install the toolchain first (bash setup/bootstrap.sh) or export JAVA_HOME, HADOOP_HOME,"
            echo "PIG_HOME, HIVE_HOME and SPARK_HOME and rerun."
            exit 1
        fi
    fi
    # shellcheck disable=SC1090
    source "$CONFIG"
    PROJECT="$root"
    export PROJECT_HOME="$root"
    mkdir -p "$PROJECT/logs"

    # Chunked processing (see scripts/split_data.sh and scripts/run_pig_parts.sh).
    # All three can be overridden from the environment or config/fraudlens.env.
    local rows_user="${ROWS_PER_PART:-}"                                      # a value from the user (or the parent script)
    ROWS_PER_PART="${ROWS_PER_PART:-600000}"                                  # 0 = old behaviour, one file
    HDFS_RAW_PARTS_DIR="${HDFS_RAW_PARTS_DIR:-$HDFS_RAW_DIR/parts}"           # part_0001.csv, part_0002.csv, ...
    HDFS_PARTS_DIR="${HDFS_PARTS_DIR:-$(dirname "$HDFS_CLEAN_DIR")/_parts}"   # per-part Pig output (staging)

    # Sample mode: work on exactly SAMPLE_ROWS rows of the raw CSV (for example 100000) instead of all 6.36 million.
    # Everything lives in separate HDFS folders (suffix _sample<N>) and a separate Hive-done marker, so a sample
    # run never mixes with, or overwrites, the results of a full run. run_all.sh --sample-rows N turns it on and
    # remembers it in logs/active_sample_rows, so check_pipeline.sh, export_results.sh and the dashboard use the same folders.
    SAMPLE_ROWS="${SAMPLE_ROWS:-$(cat "$PROJECT/logs/active_sample_rows" 2>/dev/null || echo 0)}"
    SAMPLE_MODE="${SAMPLE_MODE:-random}"        # random = evenly spread over the file (seeded), head = the first N rows
    SAMPLE_SEED="${SAMPLE_SEED:-42}"
    HIVE_MARKER="$PROJECT/logs/hive_analysis_completed"
    if [[ ! "$SAMPLE_ROWS" =~ ^[0-9]+$ ]]; then
        echo "ERROR: SAMPLE_ROWS must be a whole number (got '$SAMPLE_ROWS')."
        exit 1
    fi
    if (( SAMPLE_ROWS > 0 )); then
        local sfx="_sample${SAMPLE_ROWS}"
        HDFS_RAW_PARTS_DIR="${HDFS_RAW_PARTS_DIR}${sfx}"
        HDFS_PARTS_DIR="${HDFS_PARTS_DIR}${sfx}"
        HDFS_CLEAN_DIR="${HDFS_CLEAN_DIR}${sfx}"
        HDFS_MODEL_DIR="${HDFS_MODEL_DIR}${sfx}"
        HDFS_PREDICTIONS_DIR="${HDFS_PREDICTIONS_DIR}${sfx}"
        HIVE_MARKER="${HIVE_MARKER}${sfx}"
        # One part is enough for a sample (a single Pig job); an explicit ROWS_PER_PART still wins.
        [[ -n "$rows_user" ]] || ROWS_PER_PART="$SAMPLE_ROWS"
    fi
    # HDFS_RAW_PARTS_DIR / HDFS_PARTS_DIR are deliberately not exported: every script recomputes them here, so the
    # sample suffix is applied exactly once. (HDFS_CLEAN_DIR etc. are reset from the config file on every load.)
    export ROWS_PER_PART HDFS_CLEAN_DIR HDFS_MODEL_DIR HDFS_PREDICTIONS_DIR SAMPLE_ROWS SAMPLE_MODE SAMPLE_SEED HIVE_MARKER
}

# Qualify an HDFS path with the default filesystem scheme: /a/b -> hdfs:///a/b
hdfs_uri() {
    case "$1" in
        *://*) printf '%s\n' "$1" ;;
        *)     printf 'hdfs://%s\n' "$1" ;;
    esac
}

# True when a Hadoop job output directory finished successfully.
hdfs_job_done() {
    hdfs dfs -test -e "$1/_SUCCESS" 2>/dev/null
}

hdfs_up() {
    hdfs dfs -test -e / 2>/dev/null
}

# Start HDFS/YARN/Hive when they are not running (idempotent).
ensure_services() {
    if hdfs_up && yarn node -list 2>/dev/null | grep -qE 'Total Nodes:[[:space:]]*[1-9]' \
       && timeout 3 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
        return 0
    fi
    echo "Hadoop/Hive services are not (fully) running - starting them ..."
    bash "$PROJECT/scripts/start_services.sh"
}

# Locate the raw PaySim CSV on the local disk. Prints the path, or returns 1.
# Search order: $DATASET_LOCAL_PATH, data/raw/paysim/, data/raw/, data/, then any match below data/.
find_local_dataset() {
    local candidate
    for candidate in \
        "${DATASET_LOCAL_PATH:-}" \
        "$PROJECT/data/raw/paysim/$DATASET_FILENAME" \
        "$PROJECT/data/raw/$DATASET_FILENAME" \
        "$PROJECT/data/$DATASET_FILENAME"
    do
        if [[ -n "$candidate" && -s "$candidate" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    candidate="$(find "$PROJECT/data" -type f -name "$DATASET_FILENAME" -size +0 2>/dev/null | head -n 1)"
    if [[ -n "$candidate" ]]; then
        printf '%s\n' "$candidate"
        return 0
    fi
    return 1
}

# Names of the raw parts stored in HDFS, one per line, in order: part_0001 part_0002 ...
list_raw_parts() {
    hdfs dfs -ls "$HDFS_RAW_PARTS_DIR" 2>/dev/null \
        | sed -n 's#.*/\(part_[0-9][0-9]*\)\.csv$#\1#p' | sort
}
