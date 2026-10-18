"""FraudLens results dashboard.

A small read-only Flask app that shows what the pipeline actually produced:
  * Hive analysis        -> hive/fraud_analysis_output.log
  * Spark metrics        -> spark/results/metrics.txt
  * Spark predictions    -> ui/data/predictions.csv   (exported from HDFS by scripts/export_results.sh)
  * Pipeline status      -> HDFS _SUCCESS markers (when the hdfs CLI is available) and the local files above

Nothing is hard-coded: when a result does not exist yet, the API says so and the page shows an empty state.
Start it with:  bash scripts/run_ui.sh
"""
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, send_from_directory

UI_DIR = Path(__file__).resolve().parent
ROOT = Path(os.environ.get("FRAUDLENS_ROOT", UI_DIR.parent)).resolve()
HIVE_LOG = Path(os.environ.get("FRAUDLENS_HIVE_LOG", ROOT / "hive" / "fraud_analysis_output.log"))
METRICS = Path(os.environ.get("FRAUDLENS_METRICS", ROOT / "spark" / "results" / "metrics.txt"))
PREDICTIONS = Path(os.environ.get("FRAUDLENS_PREDICTIONS", UI_DIR / "data" / "predictions.csv"))
CONFIG = ROOT / "config" / "fraudlens.env"
PARTS_STATE = ROOT / "logs" / "parts_state.txt"   # written by scripts/run_pig_parts.sh

app = Flask(__name__, static_folder=str(UI_DIR / "static"), static_url_path="/static")


# ---------------------------------------------------------------- helpers
def read_config():
    cfg = {
        "HDFS_RAW_DIR": "/fraudlens/raw",
        "HDFS_CLEAN_DIR": "/fraudlens/processed/paysim_cleaned",
        "HDFS_MODEL_DIR": "/fraudlens/models/spark_fraud_rf_model",
        "HDFS_PREDICTIONS_DIR": "/fraudlens/results/spark_fraud_rf_predictions",
        "DATASET_FILENAME": "PS_20174392719_1491204439457_log.csv",
    }
    if CONFIG.exists():
        for line in CONFIG.read_text().splitlines():
            m = re.match(r"\s*export\s+([A-Z_]+)=(.*)$", line)
            if m and "$" not in m.group(2):
                cfg[m.group(1)] = m.group(2).strip().strip("'\"")
    # A run_all.sh --sample-rows N run keeps its output in folders ending in _sample<N> (see scripts/lib.sh).
    try:
        sample = int((ROOT / "logs" / "active_sample_rows").read_text().strip())
    except (OSError, ValueError):
        sample = 0
    cfg["SAMPLE_ROWS"] = sample
    if sample > 0:
        for key in ("HDFS_CLEAN_DIR", "HDFS_MODEL_DIR", "HDFS_PREDICTIONS_DIR"):
            cfg[key] = f"{cfg[key]}_sample{sample}"
    return cfg


def read_parts_state():
    """How many of the raw-data parts have been cleaned (None when the pipeline never ran in parts)."""
    try:
        d = dict(l.strip().split("=", 1) for l in PARTS_STATE.read_text().splitlines() if "=" in l)
        return {"done": int(d["parts_done"]), "total": int(d["parts_total"]),
                "rows_per_part": int(d.get("rows_per_part", 0)), "updated": d.get("updated")}
    except (OSError, KeyError, ValueError):
        return None


def mtime(path):
    try:
        return time.strftime("%Y-%m-%d %H:%M", time.localtime(Path(path).stat().st_mtime))
    except OSError:
        return None


def num(text):
    try:
        v = float(text)
        return int(v) if v.is_integer() and "." not in text and "E" not in text.upper() else v
    except (TypeError, ValueError):
        return text


# ---------------------------------------------------------------- Hive log
def parse_hive(path=HIVE_LOG):
    """Return the beeline result tables found in the log as {name: [row dicts]}."""
    if not Path(path).exists():
        return None
    tables, header, rows = [], None, None
    for raw in Path(path).read_text(errors="replace").splitlines():
        s = raw.strip()
        if s.startswith("+--"):                     # table border
            continue
        if s.startswith("|") and s.endswith("|"):   # header or data row
            cells = [c.strip() for c in s.strip("|").split("|")]
            if header is None:
                header, rows = [c.lower() for c in cells], []
            else:
                rows.append(dict(zip(header, [num(c) for c in cells])))
        elif header is not None:                    # first non-table line ends the table
            tables.append((header, rows)); header, rows = None, None
    if header is not None:
        tables.append((header, rows))
    out = {}
    for header, rows in tables:
        cols = set(header)
        if not rows: continue
        if "frauds_with_zero_origin_balance" in cols: name = "zero_balance"
        elif "minimum_amount" in cols: name = "fraud_amounts"
        elif {"type", "isflaggedfraud"} <= cols: name = "type_flag"
        elif "isflaggedfraud" in cols: name = "flagged"
        elif "type" in cols and "fraud_percentage" in cols: name = "by_type"
        elif "legitimate_transactions" in cols and "fraud_percentage" in cols: name = "overview"
        else: continue
        out.setdefault(name, rows)
    return out or None


# ---------------------------------------------------------------- Spark metrics
def parse_metrics(path=METRICS):
    if not Path(path).exists():
        return None
    text = Path(path).read_text(errors="replace")
    m = {"model": None, "thresholds": [], "importances": [], "raw": {}}
    section = None
    for line in text.splitlines():
        s = line.strip()
        if not s:
            section = None
            continue
        if s.startswith("Model:"):
            m["model"] = s[len("Model:"):].strip(); continue
        if s.startswith("Feature importances"):
            section = "imp"; continue
        if s.startswith("Precision/recall by probability threshold"):
            section = "thr"; continue
        if section == "imp":
            parts = s.rsplit(None, 1)
            if len(parts) == 2:
                try: m["importances"].append({"feature": parts[0], "importance": float(parts[1])})
                except ValueError: pass
            continue
        t = re.match(r"threshold ([0-9.]+): precision=([0-9.]+) recall=([0-9.]+) flagged=(\d+) \(TP=(\d+) FP=(\d+)\)", s)
        if t:
            m["thresholds"].append({"threshold": float(t.group(1)), "precision": float(t.group(2)),
                                    "recall": float(t.group(3)), "flagged": int(t.group(4)),
                                    "tp": int(t.group(5)), "fp": int(t.group(6))})
            continue
        kv = re.match(r"([A-Za-z0-9 \-_/%]+):\s*(.+)$", s)
        if kv:
            m["raw"][kv.group(1).strip()] = kv.group(2).strip()
    r = m["raw"]
    def f(key):
        try: return float(r[key])
        except (KeyError, ValueError): return None
    def i(key):
        try: return int(float(r[key]))
        except (KeyError, ValueError): return None
    m["summary"] = {
        "total": i("Total valid records"), "fraud": i("Fraud records"), "legit": i("Legitimate records"),
        "fraud_pct": f("Fraud percentage") if f("Fraud percentage") is not None else
                     (float(r["Fraud percentage"].rstrip("%")) if "Fraud percentage" in r else None),
        "train": i("Training records"), "test": i("Testing records"),
        "tn": i("True negatives"), "fp": i("False positives"), "fn": i("False negatives"), "tp": i("True positives"),
        "accuracy": f("Accuracy"), "precision": f("Fraud precision"), "recall": f("Fraud recall"),
        "f1": f("Fraud F1-score"), "roc_auc": f("ROC-AUC"), "pr_auc": f("PR-AUC"),
    }
    m["is_random_forest"] = bool(m["model"] and "random forest" in m["model"].lower())
    m["updated"] = mtime(path)
    return m


# ---------------------------------------------------------------- predictions
_cache = {"key": None, "df": None, "curve": None}


def load_predictions():
    if not PREDICTIONS.exists():
        return None
    key = (PREDICTIONS.stat().st_mtime, PREDICTIONS.stat().st_size)
    if _cache["key"] == key:
        return _cache["df"]
    df = pd.read_csv(
        PREDICTIONS,
        usecols=["step", "type", "amount", "isFraud", "predictedFraud", "fraudProbability"],
        dtype={"step": "int32", "type": "category", "amount": "float32", "isFraud": "int8",
               "predictedFraud": "int8", "fraudProbability": "float32"},
    )
    _cache.update(key=key, df=df, curve=None)
    return df


def stats_at(df, t):
    flagged = df["fraudProbability"].to_numpy() >= t
    y = df["isFraud"].to_numpy() == 1
    tp = int((flagged & y).sum()); fp = int((flagged & ~y).sum())
    fn = int((~flagged & y).sum()); tn = int((~flagged & ~y).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"threshold": round(float(t), 4), "tp": tp, "fp": fp, "fn": fn, "tn": tn, "precision": precision,
            "recall": recall, "f1": f1, "flagged": tp + fp,
            "false_alarms_per_fraud": (fp / tp) if tp else None}


def unavailable(what, hint):
    return jsonify({"available": False, "what": what, "hint": hint})


# ---------------------------------------------------------------- routes
@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/overview")
def api_overview():
    hive, metrics, df = parse_hive(), parse_metrics(), load_predictions()
    out = {"available": any([hive, metrics, df is not None]), "hive_updated": mtime(HIVE_LOG) if hive else None}
    if hive and "overview" in hive:
        out["dataset"] = hive["overview"][0]
    if hive and "by_type" in hive:
        out["by_type"] = hive["by_type"]
    if metrics:
        out["metrics"] = metrics["summary"]; out["model"] = metrics["model"]
        out["is_random_forest"] = metrics["is_random_forest"]; out["metrics_updated"] = metrics["updated"]
    if df is not None:
        n = len(df)
        out["test"] = {"rows": n, "actual_fraud": int(df["isFraud"].sum()),
                       "predicted_fraud": int(df["predictedFraud"].sum())}
        day = (df["step"] - 1) // 24 + 1
        g = pd.DataFrame({"day": day, "actual": df["isFraud"], "predicted": df["predictedFraud"]}).groupby("day").sum()
        out["trend"] = [{"day": int(d), "actual": int(r.actual), "predicted": int(r.predicted)} for d, r in g.iterrows()]
    if not out["available"]:
        out["hint"] = "No results yet. Run: bash scripts/run_all.sh, then bash scripts/export_results.sh"
    return jsonify(out)


@app.route("/api/hive")
def api_hive():
    hive = parse_hive()
    if not hive:
        return unavailable("Hive analysis", "Run the Hive stage (bash scripts/run_hive.sh) to create hive/fraud_analysis_output.log")
    return jsonify({"available": True, "updated": mtime(HIVE_LOG), "tables": hive})


@app.route("/api/model")
def api_model():
    m = parse_metrics()
    if not m:
        return unavailable("Spark metrics", "Run the Spark stage (bash spark/run_fraud_model.sh) to create spark/results/metrics.txt")
    df = load_predictions()
    return jsonify({"available": True, **{k: m[k] for k in ("model", "summary", "thresholds", "importances", "is_random_forest", "updated")},
                    "predictions_loaded": df is not None})


@app.route("/api/threshold")
def api_threshold():
    df = load_predictions()
    if df is None:
        return unavailable("Predictions", "Run bash scripts/export_results.sh to copy the predictions from HDFS to ui/data/predictions.csv")
    t = min(max(float(request.args.get("t", 0.5)), 0.0), 1.0)
    if _cache["curve"] is None:
        _cache["curve"] = [stats_at(df, x) for x in np.linspace(0.02, 0.98, 49)]
    return jsonify({"available": True, "at": stats_at(df, t), "curve": _cache["curve"]})


MASKS = {
    "pred_fraud": lambda d: d["predictedFraud"] == 1,
    "pred_legit": lambda d: d["predictedFraud"] == 0,
    "actual_fraud": lambda d: d["isFraud"] == 1,
    "tp": lambda d: (d["isFraud"] == 1) & (d["predictedFraud"] == 1),
    "fp": lambda d: (d["isFraud"] == 0) & (d["predictedFraud"] == 1),
    "fn": lambda d: (d["isFraud"] == 1) & (d["predictedFraud"] == 0),
    "tn": lambda d: (d["isFraud"] == 0) & (d["predictedFraud"] == 0),
}


def query_frame(df, args):
    mask = pd.Series(True, index=df.index)
    typ = args.get("type", "")
    if typ and typ != "all":
        mask &= df["type"] == typ
    status = args.get("status", "all")
    if status in MASKS:
        mask &= MASKS[status](df)
    for name, col, op in (("min_amount", "amount", "ge"), ("max_amount", "amount", "le"), ("min_prob", "fraudProbability", "ge")):
        raw = args.get(name, "")
        if raw not in ("", None):
            try:
                v = float(raw)
                mask &= (df[col] >= v) if op == "ge" else (df[col] <= v)
            except ValueError:
                pass
    return df[mask]


def rows_json(sub):
    return [{"step": int(r.step), "type": str(r.type), "amount": float(r.amount), "actual": int(r.isFraud),
             "predicted": int(r.predictedFraud), "probability": float(r.fraudProbability)}
            for r in sub.itertuples(index=False)]


@app.route("/api/transactions")
def api_transactions():
    df = load_predictions()
    if df is None:
        return unavailable("Predictions", "Run bash scripts/export_results.sh to copy the predictions from HDFS to ui/data/predictions.csv")
    sub = query_frame(df, request.args)
    sort = request.args.get("sort", "probability")
    col = {"probability": "fraudProbability", "amount": "amount", "step": "step"}.get(sort, "fraudProbability")
    asc = request.args.get("order", "desc") == "asc"
    size = min(max(int(request.args.get("size", 25)), 1), 200)
    page = max(int(request.args.get("page", 1)), 1)
    total = len(sub)
    top = sub.nsmallest(page * size, col) if asc else sub.nlargest(page * size, col)
    page_rows = top.iloc[(page - 1) * size: page * size]
    return jsonify({"available": True, "total": int(total), "page": page, "size": size,
                    "pages": max(1, -(-total // size)), "rows": rows_json(page_rows),
                    "types": sorted(map(str, df["type"].cat.categories))})


@app.route("/api/alerts")
def api_alerts():
    df = load_predictions()
    if df is None:
        return unavailable("Predictions", "Run bash scripts/export_results.sh to copy the predictions from HDFS to ui/data/predictions.csv")
    t = float(request.args.get("t", 0.5))
    limit = min(max(int(request.args.get("limit", 50)), 1), 200)
    typ = request.args.get("type", "all")
    sub = df[df["fraudProbability"] >= t]
    if typ != "all":
        sub = sub[sub["type"] == typ]
    top = sub.nlargest(limit, "fraudProbability")
    return jsonify({"available": True, "total_alerts": int(len(sub)),
                    "confirmed": int(sub["isFraud"].sum()), "rows": rows_json(top),
                    "types": sorted(map(str, df["type"].cat.categories))})


def hdfs_has(path):
    if not shutil.which("hdfs"):
        return None
    try:
        r = subprocess.run(["hdfs", "dfs", "-test", "-e", path], capture_output=True, timeout=8)
        return r.returncode == 0
    except (subprocess.TimeoutExpired, OSError):
        return None


@app.route("/api/pipeline")
def api_pipeline():
    cfg = read_config()
    hive, metrics = parse_hive(), parse_metrics()
    df_exists = PREDICTIONS.exists()
    parts = read_parts_state()
    raw_full = f"{cfg['HDFS_RAW_DIR']}/{cfg['DATASET_FILENAME']}"
    sfx = f"_sample{cfg['SAMPLE_ROWS']}" if cfg.get("SAMPLE_ROWS") else ""
    raw_parts = f"{cfg['HDFS_RAW_DIR']}/parts{sfx}/_SPLIT_DONE"
    has_full, has_parts = hdfs_has(raw_full), hdfs_has(raw_parts)
    raw_ok = None if (has_full is None and has_parts is None) else bool(has_full or has_parts)
    checks = [
        ("Raw data in HDFS", "HDFS", raw_ok, raw_full if has_full or not has_parts else f"{cfg['HDFS_RAW_DIR']}/parts{sfx}/", None),
        ("Pig cleaning", "Pig", hdfs_has(f"{cfg['HDFS_CLEAN_DIR']}/_SUCCESS"), cfg["HDFS_CLEAN_DIR"], None),
        ("Hive analytics", "Hive", bool(hive), "hive/fraud_analysis_output.log", mtime(HIVE_LOG) if hive else None),
        ("Spark model", "Spark", hdfs_has(f"{cfg['HDFS_MODEL_DIR']}/metadata/_SUCCESS"), cfg["HDFS_MODEL_DIR"], None),
        ("Spark evaluation", "Spark", bool(metrics), "spark/results/metrics.txt" + (f" ({metrics['model']})" if metrics and metrics['model'] else ""), mtime(METRICS) if metrics else None),
        ("Predictions", "Spark", hdfs_has(f"{cfg['HDFS_PREDICTIONS_DIR']}/_SUCCESS"), cfg["HDFS_PREDICTIONS_DIR"], None),
        ("Predictions exported for this UI", "UI", df_exists, "ui/data/predictions.csv", mtime(PREDICTIONS) if df_exists else None),
    ]
    stages = []
    for name, tool, ok, where, when in checks:
        status = "done" if ok else ("unknown" if ok is None else "missing")
        stages.append({"name": name, "tool": tool, "status": status, "where": where, "updated": when})
    if parts:
        # Pig works in parts of N rows; show how far it got (partial = results cover only the finished parts).
        status = "done" if parts["done"] >= parts["total"] else ("partial" if parts["done"] else "missing")
        stages.insert(2, {"name": "Data parts cleaned", "tool": "Pig", "status": status,
                          "where": f"{parts['done']} of {parts['total']} parts of {parts['rows_per_part']:,} rows",
                          "updated": parts["updated"]})
    return jsonify({"available": True, "stages": stages, "hdfs_cli": bool(shutil.which("hdfs")), "parts": parts})


@app.route("/api/parts")
def api_parts():
    """Light endpoint for the banner shown when the results cover only some of the data parts."""
    return jsonify({"parts": read_parts_state()})


if __name__ == "__main__":
    host = os.environ.get("UI_HOST", "127.0.0.1")
    port = int(os.environ.get("UI_PORT", "8501"))
    print(f"FraudLens dashboard: http://{host}:{port}   (root: {ROOT})")
    app.run(host=host, port=port, debug=False, threaded=True)
