#!/usr/bin/env bash
# Read-only snapshot of everything that matters when a job hangs (YARN jobs stuck in PREP/ACCEPTED, safe mode, ...).
set -uo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$PROJECT_HOME/scripts/lib.sh"
load_config

echo "== Memory =="; free -m
echo; echo "== Java processes =="; jps 2>/dev/null || ps -eo pid,args | grep -E 'NameNode|DataNode|ResourceManager|NodeManager|HiveServer2|HiveMetaStore' | grep -v grep
echo; echo "== HDFS =="; hdfs dfsadmin -safemode get 2>&1 | head -2; hdfs dfsadmin -report 2>/dev/null | grep -E 'DFS (Used|Remaining)|Live datanodes' | head -4
echo; echo "== YARN nodes (memory capacity) =="; yarn node -list -all 2>&1 | tail -5
for n in $(yarn node -list 2>/dev/null | awk 'NR>2 {print $1}'); do yarn node -status "$n" 2>/dev/null | grep -E 'Memory-Capacity|Memory-Used|CPU-Capacity'; done
echo; echo "== YARN applications =="; yarn application -list -appStates ALL 2>&1 | tail -8
echo; echo "== Effective memory settings =="
for f in yarn-site.xml mapred-site.xml capacity-scheduler.xml; do
    echo "-- $f"; grep -A1 -E 'memory|maximum-am|vmem|scheduler.class|aux-services' "$HADOOP_CONF_DIR/$f" 2>/dev/null | grep -E 'name|value' | sed 's/^[[:space:]]*//' | paste - - | head -14
done
echo
echo "Hints:"
echo " * Jobs stuck in PREP/ACCEPTED: NodeManager memory too small for the ApplicationMaster, or max-am-resource-percent too low."
echo "   Fix: bash setup/configure_hadoop.sh --force   then   bash scripts/start_services.sh (restart YARN: stop-yarn.sh; start-yarn.sh)"
echo " * Kill a stuck job:  yarn application -kill <application_id>"
echo " * Lower memory use: export PIG_TASK_MB=768 (default 1024) before scripts/run_pig.sh"
