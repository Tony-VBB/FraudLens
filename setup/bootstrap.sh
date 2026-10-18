#!/usr/bin/env bash
# One-shot setup for a brand-new Ubuntu machine after `git clone`:
#   bash setup/bootstrap.sh
# Every step is idempotent, so it is safe to rerun after a failure.
# SKIP_APT=1 skips the apt step (no sudo); SKIP_TOOLCHAIN=1 skips the downloads (tools already installed).
set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT"
chmod +x scripts/*.sh setup/*.sh spark/*.sh 2>/dev/null || true

step() { printf '\n########## %s ##########\n' "$1"; }

if [[ "${SKIP_APT:-0}" != 1 ]]; then step "1/6 System packages";  bash setup/install_dependencies.sh; fi
if [[ "${SKIP_TOOLCHAIN:-0}" != 1 ]]; then step "2/6 Hadoop, Pig, Hive, Spark"; bash setup/install_toolchain.sh; fi
step "3/6 Environment config";  bash setup/configure_environment.sh
step "4/6 Hadoop/YARN config";  bash setup/configure_hadoop.sh
step "5/6 Hive config";         bash setup/configure_hive.sh
step "6/6 Verification";        bash setup/verify_installation.sh

cat <<MSG

Bootstrap finished. Next:
  1. Put PS_20174392719_1491204439457_log.csv in  $PROJECT/data/raw/paysim/
  2. bash scripts/run_all.sh        (starts services automatically)
MSG
