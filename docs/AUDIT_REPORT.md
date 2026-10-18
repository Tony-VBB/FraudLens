> **Note:** findings below describe the original archive. See `../CHANGELOG.md` for how each item was resolved.

# FraudLens — Audit Report

Scope: every file in `bigdata_project.zip` (scripts, Pig/Hive/Spark code, logs, backups, saved model metadata).
Method: static read-through, `bash -n` on all shell scripts (all pass), `git check-ignore` against the shipped
`.gitignore`, cross-checking paths between components, arithmetic checks on the reported metrics, and reading the
Pig/Hive/Spark logs. Nothing was executed against Hadoop (no cluster here), so items marked *unverified* need a
check on your VM.

## 0. What is correct

- Hive output matches the README: 6,101,255 rows, 7,820 fraud (0.1282 %).
- Spark metrics are internally consistent: TN+FP+FN+TP = 1,221,105 = test count; train+test = 6,101,255;
  recall 1425/1575 = 0.9048; precision 1425/171,342 = 0.0083; accuracy 0.8607. README table = `metrics.txt` = `spark_debug.log`.
- Schemas agree across Pig output (13 fields), Hive DDL (13 columns) and Spark `StructType` (13 fields), same order.
- Spark script correctly excludes account IDs and `isFlaggedFraud`, uses class weights, a seeded split, and refuses to overwrite outputs.
- All 8 shell scripts pass `bash -n`.

## 1. Critical

### C-1 Pig writes to a test path; the rest of the pipeline reads the production path
`pig/clean_paysim.pig` line 55: `STORE ... INTO '/fraudlens_test/processed/paysim_cleaned'`.
Hive (`LOCATION '/fraudlens/processed/paysim_cleaned'`), `fraud_model.py` (`hdfs:///fraudlens/processed/...`),
`run_fraud_model.sh`, `run_all.sh` (`$HDFS_CLEAN_DIR`) and the README all use `/fraudlens/processed/...`.
The backup `clean_paysim.pig.before_test` has the correct path — the test edit was never reverted.

Effects:
- Fresh machine: `run_all.sh` sees no `$HDFS_CLEAN_DIR`, runs Pig, Pig writes to `/fraudlens_test/...`, Hive builds an empty table, Spark runner aborts ("Cleaned HDFS data not found").
- Re-running Pig on the dev VM fails with *Output directory ... already exists* (this is exactly `pig_1792215903698.log`).
- The recorded results exist only because a correct earlier output was already sitting in `/fraudlens/processed/`.

Fix: `INTO '/fraudlens/processed/paysim_cleaned'` (better: pass `-param OUT=$HDFS_CLEAN_DIR` and use `'$OUT'`; same for the input).

## 2. High

### H-1 `.gitignore` excludes the Hive scripts and test data
`hive/` (line 41) ignores the whole directory, so `fraud_analysis.hql` is never committed. `*.csv` also ignores `test/sample.csv`;
`models/` + `artifacts/` ignore everything the README says is "exported". Fix: anchor patterns (`/hadoop/`, `/pig-*/`, `/spark-*-bin-*/`) instead of `hive/`, add `!test/sample.csv`.
The file also contains two near-duplicate blocks (config, datasets, models, archives, `.bak`, `*.before_*` all listed twice).

### H-2 Cleaned row count is ~4 % below the stock dataset (*unverified, probable bug*)
Stock PaySim (`PS_20174392719_1491204439457_log.csv`) has 6,362,620 rows, 8,213 frauds, 16 flagged. The pipeline reports
6,101,255 rows, 7,820 frauds, 10 flagged — **261,365 rows and 393 frauds lost**. Also the max fraud amount is 9,996,886.64 whereas the raw maximum is 10,000,000.
Likely cause: the Pig filter regex `'[0-9]+(\\.[0-9]+)?'` rejects values written in scientific notation (e.g. `1.0E7`), which dataset
writers emit for numbers ≥ 10^7, so every row with any such field is silently dropped — biasing the model against the largest transactions.
Check on the VM:
```bash
hdfs dfs -cat /fraudlens/raw/*.csv | grep -cE '[0-9]E[0-9]'          # rows with sci-notation
hdfs dfs -cat /fraudlens/raw/*.csv | wc -l                           # expect 6362621 (incl. header)
```
Fix: allow an exponent in the regex (`'[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?'`) or drop regex validation and cast with `(double)`, filtering nulls instead. Re-run and update README/metrics.

### H-3 Hard-coded machine paths
- `scripts/run_all.sh`, `scripts/start_services.sh`, `setup/verify_installation.sh`: `PROJECT="$HOME/FraudLens"` (ignore `PROJECT_HOME`).
- `spark/fraud_model.py`: `LOCAL_RESULTS = ~/FraudLens/spark/results/metrics.txt`.
- `spark/run_model_debug.sh`: Java, Hadoop, Spark paths and `$HOME/FraudLens/...` baked in.
- `test/pig_test.pig`: `/home/balaji/FraudLens/test/sample.csv`.
- The user ran Pig from `/home/balaji/Desktop/bigdata_project/` (see Pig logs), i.e. the repo is *not* at `~/FraudLens`, so these scripts can't work from the checked-out location without copying/symlinking.

Fix: `PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"` in every script (as `configure_environment.sh` already does), and `os.environ.get("PROJECT_HOME", ...)` in Python.

### H-4 Spark runner depends on a Pig-internal file name
`run_fraud_model.sh`: `hdfs dfs -test -e "$HDFS_CLEAN_DIR/part-r-00000"`. That name exists only for MapReduce reducer output with one reducer;
with several reducers, Tez, or local mode it differs, and the check fails although data is present. Use `-test -e "$HDFS_CLEAN_DIR/_SUCCESS"`.
The same fragility applies to the "skip Pig if directory exists" guard in `run_all.sh`: a half-written or empty directory counts as done. Check `_SUCCESS` instead.

## 3. Medium

| ID | Finding | Fix |
|---|---|---|
| M-1 | `PROJECT_HOME` is exported by the config and honoured by `prepare_data.sh`/`run_fraud_model.sh` only; others ignore it | See H-3 |
| M-2 | `start_services.sh` redirects to `$PROJECT/logs/*.log` but never `mkdir -p logs`; with `set -e` the first Hive start fails on a fresh clone | add `mkdir -p "$PROJECT/logs"` |
| M-3 | `spark/results/spark_run.log` is a **failed** run (`ModuleNotFoundError: numpy`, 05:24); the successful run (metrics.txt) came from `run_model_debug.sh` → `spark_debug.log`. The official runner has no recorded successful log | rerun `run_fraud_model.sh` and commit/refresh logs |
| M-4 | README mentions "Spark setup notes" for a `distutils.LooseVersion` patch — no such notes or patch exist; README also lists `artifacts/` (absent). Python 3.12 removed `distutils`, so a stock Spark 3.5 + Python 3.12 setup needs `setuptools` or a patch | document the patch or install `python3-setuptools`; fix the artifacts path (model sits in top-level `models/`) |
| M-5 | `install_dependencies.sh` installs neither Java 8, Python 3.12, nor any of Hadoop/Pig/Hive/Spark ("toolchain setup must…") — that setup doesn't exist in the repo; it also omits `setuptools` | add a toolchain installer or document manual download links and versions |
| M-6 | `configure_environment.sh`: `SPARK_PATH="$(SPARK_HOME= find_tool SPARK_HOME ...)"` blanks the variable, so a user-supplied `SPARK_HOME` is always ignored (added in `.before_spark_fix`) | unset only the pre-existing *Spark env* if needed, otherwise drop the prefix |
| M-7 | Default `HIVE_HOME=~/hive`, but the Hive log shows Hive running from `/home/balaji/Desktop/Big_data%20_lab_7/apache-hive-4.0.1-bin` (URL-encoded space in path). Also duplicate SLF4J bindings (Hive vs Hadoop) | standardise install dir; remove one binding jar |
| M-8 | Config variables (`HDFS_CLEAN_DIR`, `HDFS_MODEL_DIR`, …) are used by shell checks but **not** by `clean_paysim.pig`, `fraud_analysis.hql`, `fraud_model.py`, which hard-code paths. Any change to the config silently desynchronises them (C-1 is an instance of this) | parameterise: `pig -param`, `beeline --hivevar`, `os.environ` |
| M-9 | `run_all.sh`: Hive skip logic uses `grep -q "7820"` in an old log — brittle and dataset-specific; it requires the local CSV even when HDFS already has the data; it never starts services; Pig failure isn't re-verified; the Hive `beeline` line has collapsed whitespace (works, hard to read) | marker file only; check `_SUCCESS`; call `start_services.sh`; restore line continuations |
| M-10 | `test/pig_test.pig` loads a local path without `-x local`/`file://`, so in MapReduce mode Pig looks in HDFS; `sample.csv` is gitignored | `pig -x local test/pig_test.pig` and `LOAD 'test/sample.csv'` |

## 4. Low / hygiene

- **Clutter in the archive:** 10 `pig_*.log` crash logs at the repo root, 17 `*.backup*` / `*.before_*` files, 3 timestamped `clean_paysim_manual_*.pig` copies, `__pycache__/*.pyc`. All are git-ignored or should be removed; keep history in git, not in file suffixes.
- `spark/run_model_debug.sh` ends with `read -r` (blocks automation) and starts without `set -euo pipefail`.
- `fraud_model.py` writes `metrics.txt` (stating "Saved predictions: …") **before** the predictions are actually written; if the CSV write fails the metrics file is misleading. Model is also saved before evaluation — a later failure leaves a partial state that the "refuse to overwrite" guard then blocks from re-running.
- `verify_installation.sh` doesn't test HDFS reachability, safe mode, or Hive/Metastore connectivity.
- `spark/README.md` and root README both document the Spark stage; versions of the "Outputs" list differ slightly (root mentions `artifacts/`).

## 5. Execution history reconstructed from the Pig logs

| Log | Failure | Meaning |
|---|---|---|
| `pig_1792205359721` | `Job in state DEFINE instead of RUNNING` | Hadoop/MR client mismatch at first run |
| `pig_1792206817810` … `206899704` (×3) | `Failed to create DataStorage` | HDFS not reachable / NameNode down |
| `pig_1792207039746` | Job `…_0011` failed, no message | MapReduce task failure (likely resources) |
| `pig_1792210237728` | `UnsetHostnameOrPort` | `fs.defaultFS` / YARN endpoint not set |
| `pig_1792215903698` | Output `/fraudlens_test/...` already exists | C-1 in action |
| `pig_1792216059059` | NameNode in **safe mode**, cannot create `/tmp` | low disk on NameNode |
| `pig_1792219667888` | Manual output dir already exists | timestamp reused |
| `pig_1792219732428` | Cannot delete output — safe mode, "Resources are low on NN" | low disk again |

Takeaway: the environment is resource-starved; add `hdfs dfsadmin -safemode get` and a free-disk check to `start_services.sh`.

## 6. Modelling observations (not bugs, worth knowing)

- Features `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest` plus the derived deltas are strongly tied to how PaySim simulates fraud (frauds drain the origin account: 7,808 / 7,820 have `newbalanceOrig = 0`). Results will not transfer to real data.
- `step` (hour index) is used as a numeric feature; it is a time counter and mostly adds noise for a linear model.
- Class weights are balanced to ~50/50, driving 170 k false positives. Report the precision-recall trade-off at several thresholds, or lower the fraud weight / pick a threshold from the PR curve.
- A single random 80/20 split; fraud rings reuse accounts, so a time-based split (by `step`) is more realistic.

## 7. Suggested fix order

1. C-1 (Pig output path) → rerun Pig on a scratch HDFS dir and diff row counts.
2. H-2 (regex) → recompute counts, Hive output, Spark metrics, update README numbers.
3. H-3/M-1/M-8 (central path handling) and H-4 (`_SUCCESS` checks).
4. H-1 `.gitignore`, then purge backups/logs from the repo.
5. M-2, M-3, M-4, M-5, M-6, M-10 and documentation.
