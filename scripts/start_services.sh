#!/usr/bin/env bash
# Start (or confirm) HDFS, YARN, Hive metastore and HiveServer2. Idempotent.
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

echo "========== FraudLens Service Startup =========="

if [[ ! -f "$HADOOP_CONF_DIR/core-site.xml" || ! -f "$HADOOP_CONF_DIR/hdfs-site.xml" ]]; then
    echo "ERROR: Hadoop configuration files are missing in $HADOOP_CONF_DIR."
    echo "Run:  bash setup/configure_hadoop.sh"
    exit 1
fi

wait_for() {   # wait_for "description" seconds command...
    local what="$1" tries="$2"; shift 2
    for _ in $(seq 1 "$tries"); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 2
    done
    echo "ERROR: Timed out waiting for $what."
    return 1
}

echo "Checking HDFS..."
if ! hdfs_up; then
    # start-dfs.sh connects over SSH, even to localhost.
    if ! ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new localhost true 2>/dev/null; then
        echo "ERROR: passwordless SSH to localhost is not working (needed by start-dfs.sh)."
        echo "Run:  bash setup/configure_hadoop.sh      (sets up the key and starts sshd)"
        exit 1
    fi
    echo "Starting HDFS..."
    "$HADOOP_HOME/sbin/start-dfs.sh"
    wait_for "HDFS" 45 hdfs dfs -test -e / || { echo "Check $HADOOP_HOME/logs/*namenode*.log"; exit 1; }
else
    echo "HDFS is already responding."
fi

# The NameNode enters safe mode when disk space is low; writes then fail.
if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "ON"; then
    echo "NameNode is in safe mode; waiting briefly in case it is still starting..."
    hdfs dfsadmin -safemode wait >/dev/null 2>&1 &
    WAIT_PID=$!
    sleep 20
    if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "ON"; then
        kill "$WAIT_PID" 2>/dev/null || true
        echo "ERROR: HDFS is still in safe mode (often caused by low disk space on the NameNode)."
        echo "Free disk space, then run:  hdfs dfsadmin -safemode leave"
        exit 1
    fi
fi

echo "Checking YARN..."
if ! yarn node -list 2>/dev/null | grep -qE 'Total Nodes:[[:space:]]*[1-9]'; then
    echo "Starting YARN..."
    "$HADOOP_HOME/sbin/start-yarn.sh"
    wait_for "a registered YARN NodeManager" 45 bash -c \
        "yarn node -list 2>/dev/null | grep -qE 'Total Nodes:[[:space:]]*[1-9]'" \
        || { echo "Check $HADOOP_HOME/logs/*nodemanager*.log and run scripts/diagnose.sh"; exit 1; }
else
    echo "YARN is already responding with a registered NodeManager."
fi

# Directories Hive and Hadoop jobs expect to exist.
hdfs dfs -mkdir -p /tmp /user/hive/warehouse >/dev/null 2>&1 || true
hdfs dfs -chmod -R g+w /tmp /user/hive/warehouse >/dev/null 2>&1 || true

echo
echo "Checking HiveServer2..."
if timeout 3 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
    echo "HiveServer2 is already listening on port 10000."
    exit 0
fi

if ! timeout 3 bash -c '</dev/tcp/localhost/9083' 2>/dev/null; then
    echo "Starting Hive metastore..."
    nohup "$HIVE_HOME/bin/hive" --service metastore \
        > "$PROJECT/logs/hive-metastore.log" 2>&1 &
    wait_for "the Hive metastore (port 9083)" 45 bash -c "timeout 2 bash -c '</dev/tcp/localhost/9083'" \
        || { echo "Review $PROJECT/logs/hive-metastore.log (schema not initialised? run setup/configure_hive.sh)"; exit 1; }
fi

echo "Starting HiveServer2..."
nohup "$HIVE_HOME/bin/hive" --service hiveserver2 \
    > "$PROJECT/logs/hiveserver2.log" 2>&1 &

echo "Waiting for HiveServer2 (can take up to 90 seconds)..."
for _ in $(seq 1 45); do
    if timeout 2 bash -c '</dev/tcp/localhost/10000' 2>/dev/null; then
        echo "HiveServer2 is listening on port 10000."
        exit 0
    fi
    sleep 2
done

echo "ERROR: HiveServer2 did not become available within 90 seconds."
echo "Review logs in $PROJECT/logs."
exit 1
