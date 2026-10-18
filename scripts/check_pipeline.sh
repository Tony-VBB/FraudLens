#!/usr/bin/env bash
# FraudLens pre-flight / post-run checker. Read-only: changes nothing in HDFS or on disk.
# Usage:  bash scripts/check_pipeline.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="$ROOT/config/fraudlens.env"
PASS=0; FAIL=0; WARN=0
ok()   { printf 'PASS  %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf 'FAIL  %s\n' "$1"; FAIL=$((FAIL+1)); }
warn() { printf 'WARN  %s\n' "$1"; WARN=$((WARN+1)); }
has()  { grep -qF -- "$2" "$1"; }

echo "== 1. Configuration =="
if [[ -f "$CONFIG" ]]; then
    # shellcheck disable=SC1090
    source "$CONFIG"; ok "config found: $CONFIG"
    # Same folders as the last run_all.sh: a --sample-rows run uses folders ending in _sample<N> (see scripts/lib.sh).
    SAMPLE_ROWS="${SAMPLE_ROWS:-$(cat "$ROOT/logs/active_sample_rows" 2>/dev/null || echo 0)}"
    HDFS_RAW_PARTS_DIR="${HDFS_RAW_PARTS_DIR:-$HDFS_RAW_DIR/parts}"
    if [[ "$SAMPLE_ROWS" =~ ^[0-9]+$ ]] && (( SAMPLE_ROWS > 0 )); then
        for v in HDFS_RAW_PARTS_DIR HDFS_CLEAN_DIR HDFS_MODEL_DIR HDFS_PREDICTIONS_DIR; do
            printf -v "$v" '%s' "${!v}_sample${SAMPLE_ROWS}"
        done
        ok "sample mode: $SAMPLE_ROWS rows (HDFS folders end in _sample$SAMPLE_ROWS)"
    fi
else
    bad "missing $CONFIG (run setup/configure_environment.sh)"; exit 1
fi
for v in JAVA_HOME HADOOP_HOME PIG_HOME HIVE_HOME SPARK_HOME HDFS_RAW_DIR HDFS_CLEAN_DIR \
         HDFS_MODEL_DIR HDFS_PREDICTIONS_DIR DATASET_FILENAME HIVE_JDBC_URL SPARK_MASTER \
         RF_NUM_TREES RF_MAX_DEPTH; do
    [[ -n "${!v:-}" ]] && ok "$v set" || bad "$v not set (regenerate config with setup/configure_environment.sh)"
done
if [[ -x "$JAVA_HOME/bin/java" ]] && "$JAVA_HOME/bin/java" -version 2>&1 | grep -q 'version "1\.8\.'; then
    ok "Java 8"
else
    bad "Java 8 not found at JAVA_HOME"
fi
PY="${PYSPARK_PYTHON:-python3.12}"
"$PY" -c 'import numpy, packaging' 2>/dev/null && ok "$PY has numpy + packaging" || bad "$PY lacks numpy/packaging"
"$PY" -c 'import setuptools' 2>/dev/null && ok "setuptools present (distutils shim)" \
    || warn "setuptools missing: PySpark 3.5 may fail on Python 3.12 (LooseVersion)"

echo "== 2. Cross-file consistency (static) =="
has "$ROOT/scripts/run_pig.sh" 'OUT_PATH=$OUT_PATH' && has "$ROOT/scripts/run_pig.sh" 'hdfs_uri "$OUT_HDFS"' && has "$ROOT/scripts/run_pig.sh" 'OUT_HDFS="${2:-$HDFS_CLEAN_DIR}"' \
    && ok "run_pig.sh writes to HDFS_CLEAN_DIR (or the output given as argument)" || bad "run_pig.sh does not use HDFS_CLEAN_DIR"
has "$ROOT/scripts/run_pig.sh" '-x mapreduce' && ok "run_pig.sh selects mapreduce mode explicitly" || bad "run_pig.sh does not pass -x mapreduce"
grep -qE '"\$PIG_HOME/bin/pig [^"]' "$ROOT/scripts/run_pig.sh" && bad "run_pig.sh glues options into the pig executable path" || ok "pig executable and options are separate words"
has "$ROOT/pig/clean_paysim.pig" "INTO '\$OUT_PATH'" && ok "Pig STORE uses \$OUT_PATH (no hard-coded path)" \
    || bad "Pig STORE path is hard-coded"
has "$ROOT/pig/clean_paysim.pig" "LOAD '\$RAW_PATH'" && ok "Pig LOAD uses \$RAW_PATH" || bad "Pig LOAD path is hard-coded"
has "$ROOT/hive/fraud_analysis.hql" '${hivevar:HDFS_CLEAN_DIR}' && ok "Hive LOCATION uses hivevar HDFS_CLEAN_DIR" \
    || bad "Hive LOCATION is hard-coded"
has "$ROOT/spark/fraud_model.py" 'os.environ.get("HDFS_CLEAN_DIR"' && ok "Spark reads HDFS_CLEAN_DIR from the environment" \
    || bad "Spark input path is hard-coded"
grep -qE "[[:space:]]LOAD '[^\$]|INTO '[^\$]" "$ROOT/pig/clean_paysim.pig" && bad "literal path found in Pig script" || ok "no literal HDFS paths in Pig script"
grep -rIl --include='*.sh' --include='*.py' --include='*.pig' --include='*.hql' -e '/home/[a-z]' "$ROOT" 2>/dev/null \
    | grep -v '/docs/' | grep -q . && bad "hard-coded /home/<user> path found" || ok "no hard-coded home directories"
if git -C "$ROOT" rev-parse >/dev/null 2>&1; then
    git -C "$ROOT" check-ignore -q hive/fraud_analysis.hql && bad ".gitignore ignores hive/fraud_analysis.hql" || ok "hive script is not git-ignored"
    git -C "$ROOT" check-ignore -q test/sample.csv && bad ".gitignore ignores test/sample.csv" || ok "test sample is not git-ignored"
fi

echo "== 3. Services =="
if hdfs dfs -test -e / 2>/dev/null; then
    ok "HDFS reachable"
    hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && ok "HDFS safe mode OFF" || bad "HDFS safe mode ON (writes will fail)"
else
    bad "HDFS not reachable (scripts/start_services.sh)"
fi
yarn node -list >/dev/null 2>&1 && ok "YARN responding" || warn "YARN not responding"
timeout 3 bash -c '</dev/tcp/localhost/10000' 2>/dev/null && ok "HiveServer2 :10000" || warn "HiveServer2 not listening"

echo "== 4. Pipeline artefacts =="
[[ -s "$ROOT/data/raw/paysim/$DATASET_FILENAME" ]] && ok "local raw CSV present" || warn "local raw CSV missing (only needed for upload)"
RAWN=""
if hdfs dfs -test -e "$HDFS_RAW_DIR/$DATASET_FILENAME" 2>/dev/null; then
    ok "raw data in HDFS"
    RAWN=$(( $(hdfs dfs -cat "$HDFS_RAW_DIR/$DATASET_FILENAME" 2>/dev/null | wc -l) - 1 ))
    echo "      raw data rows (without header): $RAWN"
elif hdfs dfs -test -e "$HDFS_RAW_PARTS_DIR/_SPLIT_DONE" 2>/dev/null; then
    ok "raw data in HDFS (split into parts)"
    RAWN=$(hdfs dfs -cat "$HDFS_RAW_PARTS_DIR"/part_*.csv 2>/dev/null | wc -l)    # the parts have no header line
    echo "      raw data rows (without header): $RAWN"
else
    warn "raw data not in HDFS"
fi
if hdfs dfs -test -e "$HDFS_CLEAN_DIR/_SUCCESS" 2>/dev/null; then
    ok "Pig output complete (_SUCCESS)"
    STATS=$(hdfs dfs -cat "$HDFS_CLEAN_DIR"/*part-* 2>/dev/null | awk -F, '{n++; f+=$12; c=NF} END{print n+0, f+0, c+0}')
    read -r N F COLS <<< "$STATS"
    echo "      cleaned rows=$N fraud=$F columns=$COLS"
    [[ "$COLS" == 13 ]] && ok "13 columns" || bad "expected 13 columns, found $COLS"
    if [[ -n "$RAWN" && "$RAWN" -gt 0 ]]; then
        DROP=$(( RAWN - N ))
        echo "      rows dropped by cleaning: $DROP"
        # The old regex dropped ~4 % of PaySim (scientific-notation values). After the fix only a handful should go.
        if [[ "$DROP" -gt $(( RAWN / 100 )) ]]; then
            warn "cleaning dropped more than 1 % of the raw rows; inspect the numeric filter"
        else
            ok "cleaning dropped <= 1 % of raw rows"
        fi
    fi
else
    warn "Pig output missing or incomplete: $HDFS_CLEAN_DIR"
fi
hdfs dfs -test -e "$HDFS_MODEL_DIR/metadata/_SUCCESS" 2>/dev/null && ok "Spark RF model saved" || warn "Spark RF model not found"
hdfs dfs -test -e "$HDFS_PREDICTIONS_DIR/_SUCCESS" 2>/dev/null && ok "Spark predictions saved" || warn "Spark predictions not found"
if [[ -s "$ROOT/spark/results/metrics.txt" ]]; then
    ok "metrics.txt present"
    grep -q "Random Forest" "$ROOT/spark/results/metrics.txt" && ok "metrics.txt is from the Random Forest run" \
        || warn "metrics.txt is not from a Random Forest run"
else
    warn "spark/results/metrics.txt missing (Spark stage not run yet)"
fi
if [[ -f "$ROOT/spark/results/spark_run.log" ]] && grep -q "Traceback" "$ROOT/spark/results/spark_run.log"; then
    warn "spark_run.log contains a traceback: last Spark run failed"
fi

echo
echo "Summary: $PASS passed, $WARN warnings, $FAIL failed"
[[ "$FAIL" -eq 0 ]]
