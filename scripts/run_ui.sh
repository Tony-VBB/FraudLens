#!/usr/bin/env bash
# Start the FraudLens dashboard (read-only) at http://127.0.0.1:8501
#   bash scripts/run_ui.sh              start the dashboard with the results that exist
#   bash scripts/run_ui.sh --export     first copy the predictions from HDFS (needs HDFS running)
# The first run creates a Python virtual environment in .venv-ui (needs python3-venv and internet access).
set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT"

if [[ "${1:-}" == "--export" ]]; then
    bash "$PROJECT/scripts/export_results.sh"
fi

PY="${PYSPARK_PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python3

if [[ ! -x "$PROJECT/.venv-ui/bin/python" ]]; then
    echo "Creating the dashboard virtual environment (.venv-ui) ..."
    "$PY" -m venv "$PROJECT/.venv-ui" || {
        echo "ERROR: could not create a virtual environment. Install it with:  sudo apt install python3-venv"
        exit 1
    }
fi
if ! "$PROJECT/.venv-ui/bin/python" -c 'import flask, pandas, numpy' 2>/dev/null; then
    echo "Installing dashboard dependencies (flask, pandas, numpy) ..."
    "$PROJECT/.venv-ui/bin/pip" install --quiet -r "$PROJECT/ui/requirements.txt"
fi

if [[ ! -s "$PROJECT/ui/data/predictions.csv" ]]; then
    echo "NOTE: ui/data/predictions.csv not found; the Transactions, Alerts and threshold views stay empty."
    echo "      After the Spark stage run:  bash scripts/export_results.sh"
fi

export FRAUDLENS_ROOT="$PROJECT"
exec "$PROJECT/.venv-ui/bin/python" "$PROJECT/ui/app.py"
