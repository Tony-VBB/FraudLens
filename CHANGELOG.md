# Changelog

## Revision 6 - 100,000-row sample mode and Spark fixes
- `scripts/run_all.sh --sample-rows N` (also `SAMPLE_ROWS`, `SAMPLE_MODE=random|head`, `SAMPLE_SEED`): runs split -> Pig -> Hive -> Spark on exactly N rows.
  `split_data.sh` selects the rows with seeded selection sampling (exact N, original order kept). Output goes to `*_sample<N>` HDFS folders and
  `logs/hive_analysis_completed_sample<N>`, so a sample run never mixes with a full run. The active mode is remembered in `logs/active_sample_rows`
  (a run without `--sample-rows` switches back to full) so `check_pipeline.sh`, `export_results.sh` and the dashboard read the same folders.
- Hive: the external table is now dropped and recreated on every run (metadata only), so it always points at the active folder. Before, `CREATE ... IF NOT EXISTS` would have kept pointing at the previous folder.
- Spark: `from pyspark.ml.functions import vector_to_array` imports pandas in Spark 3.5, and `setup/` never installs pandas, so the Spark stage would have
  crashed on import on a fresh machine. `fraud_model.py` now falls back to a small UDF when pandas is missing (same result).
- Spark: feature importances were printed as `feature_0 ... feature_13` because the one-hot encoder (`handleInvalid="keep"`) has one extra slot; it is now named `type=<unseen>` and all names are shown.
- `check_pipeline.sh`: counted 0 cleaned rows because it read `part-*` but the part-mode files are named `pNNNN-part-r-*`; it now reads `*part-*`, also counts raw rows from the parts and understands sample mode.
- Dashboard: reads the sample folders when a sample run is active.
- Tested: the real shell scripts and the real Spark 3.5.3 Random Forest on a synthetic 1,000,000-row PaySim-format file, sampled to 100,000 rows;
  then a full run on the same file to check that sample and full results stay separate. **Pig and Hive were not real here** (no Hadoop in the test sandbox): Pig was replaced by a
  Python script applying the same filter/derive/DISTINCT rules, and the unchanged `fraud_analysis.hql` ran on Spark SQL. Run once on your VM to confirm those two stages.

## Revision 5 - cleaning in parts of 600,000 rows with stop/resume
- New `scripts/split_data.sh`: streams the CSV into HDFS as `part_0001.csv ...` (`ROWS_PER_PART`, default 600000; no second local copy).
- New `scripts/run_pig_parts.sh`: runs Pig once per part into `HDFS_PARTS_DIR` (`.../_parts/part_NNNN`), then moves the output into
  `HDFS_CLEAN_DIR` as `pNNNN-part-r-*`, so Hive, Spark, the checker and the dashboard keep reading the same folder unchanged.
  After each part: Enter = continue, `a` = stop and analyse what is done, `q` = stop and resume later; also `logs/STOP`, `--max-parts N`,
  Ctrl-C. Finished parts are skipped on the next run; an unfinished part is discarded and redone.
- `scripts/run_all.sh` uses the chunked flow by default; new options `--max-parts`, `--no-prompt`, `--rows` (0 = old single-file flow),
  `--retrain` (delete the old Spark model/predictions when new parts were added, then train again). Without `--retrain` the old
  model is kept and a note is printed, as before. Data from an earlier single-file run is detected and not re-cleaned.
- `scripts/run_pig.sh` takes an optional input and output path (defaults unchanged); `check_pipeline.sh` static check updated to match.
- Dashboard: "Partial data" banner and a "Data parts cleaned" stage while parts are missing; "Raw data in HDFS" also accepts the parts.
- Tested with stub `hdfs`/`pig`/`beeline`/`spark-submit` (HDFS simulated on a local folder): full run, stop with `a`/`q`/`logs/STOP`/`--max-parts`,
  interactive prompt through a pseudo-terminal, resume, simulated Pig failure and recovery, `--retrain`, `--rows 0`, earlier full-run data,
  a 1.5M-row file split into 600000/600000/300000. **Not tested on a real cluster:** real Pig/Hive/Spark jobs, `hdfs dfs -put -f -` from stdin,
  and `split --filter` on your VM (needs GNU coreutils, which Ubuntu has).
- Known limits: `DISTINCT` is per part; Hive/Spark rerun over all finished parts instead of adding to earlier results.

## Revision 4 - optional results dashboard
- Added `ui/` (Flask, no external CDN, works offline) with five tabs: Overview, Transactions, Fraud alerts, Model performance
  (interactive threshold explorer), Pipeline status. It reads the Hive log, `metrics.txt` and the exported Spark predictions; empty
  states are shown for missing results, nothing is hard-coded.
- Added `scripts/export_results.sh` (HDFS predictions -> `ui/data/predictions.csv`, repeated CSV headers removed) and `scripts/run_ui.sh`
  (creates `.venv-ui`, installs dependencies, starts the app). `python3-venv` added to `install_dependencies.sh`.
- Tested in headless Chromium: all tabs, filters, pagination, threshold slider, empty states, no JavaScript errors; the Hive parser was
  tested on the real recorded Hive log; the export script on a stub HDFS. The dashboard was tested with synthetic predictions in the
  Spark output format (never shipped), so check it once against the real Random Forest output.


## Revision 3 - fresh-machine / GitHub readiness (fixes from the first real run on a VirtualBox VM)

| Problem seen | Fix |
|---|---|
| `run_all.sh` failed: `config/fraudlens.env` missing | `load_config` generates it automatically via `setup/configure_environment.sh`; clear message if tools are missing |
| HDFS/YARN not running (jobs and uploads failed) | `run_all.sh` calls `ensure_services` -> `scripts/start_services.sh` (SSH check, HDFS, safe-mode wait, YARN with NodeManager registration check, Hive dirs, metastore, HiveServer2) |
| YARN jobs frozen in `PREP` on a 4.8 GB VM | `setup/configure_hadoop.sh` sizes NodeManager/containers from RAM and raises `maximum-am-resource-percent`; `run_pig.sh` passes small container sizes (`PIG_TASK_MB`, default from RAM) via `-param`; `SET` lines in the Pig script; refuses to start with zero NodeManagers; `scripts/diagnose.sh` |
| CSV expected in `data/raw/paysim/`, user put it in `data/raw/` | `find_local_dataset` searches `data/raw/paysim/`, `data/raw/`, `data/`, `DATASET_LOCAL_PATH`; header validated before upload |
| Pig ran in LOCAL mode: `Incomplete HDFS URI, no host` | `-x mapreduce` always passed, `PIG_CLASSPATH` includes the Hadoop config directory |
| `-x mapreduce` appended to the Pig binary path | `run_pig.sh` builds the command as a Bash array (executable and options are separate words); `check_pipeline.sh` checks for this |
| Needs to work after `git pull` on a new machine | `setup/bootstrap.sh`, `install_toolchain.sh`, `configure_hadoop.sh`, `configure_hive.sh`; `.gitattributes` (LF endings); README quick start and GitHub steps |

Status: control flow, config generation (including XML validity and memory maths for a 4.8 GB machine), dataset discovery,
service auto-start and the exact Pig argument list were tested with stub tools. **Not testable here, so verify on a real machine:**
Pig's handling of the new `%default`/`SET` lines, the real downloads in `install_toolchain.sh`, `hdfs namenode -format`,
`schematool -initSchema`, and the Hadoop/Hive daemons themselves.

## Revision 2 - audit fixes and Random Forest migration

IDs refer to `docs/AUDIT_REPORT.md`. "Tested" means exercised with stub Hadoop/Pig/Hive/Spark tools and a fake HDFS directory
(control flow, paths, skip/abort logic). Pig, Hive and Spark themselves, and the Random Forest, **could not be run here** (no cluster,
no PySpark) and need one real run.

## Model: logistic regression → random forest
- `spark/fraud_model.py` now trains `RandomForestClassifier` (class-weighted, 40 trees, depth 8, 32 bins, 50 % subsampling, seed 42),
  without `StandardScaler`. Hyper-parameters are read from `RF_*` variables in `config/fraudlens.env`.
- Added precision/recall at thresholds 0.3/0.5/0.7/0.9 and ranked feature importances to `metrics.txt`.
- New HDFS output names (`spark_fraud_rf_model`, `spark_fraud_rf_predictions`) so the old logistic-regression outputs in HDFS do not
  trigger the "refuse to overwrite" guard.
- Old LR model files, metrics and log moved/removed: metrics and debug log kept under `docs/baseline_logreg/`; the saved LR model
  directory (`models/`) was deleted because it no longer matches the code.
- Verified: `py_compile` passes. Not verified: execution on Spark (needs your cluster); the first run may need tuning for time/memory.

## Fixes
| ID | Resolution | Status |
|---|---|---|
| C-1 | Pig script no longer hard-codes any path: `LOAD '$RAW_PATH'`, `STORE ... INTO '$OUT_PATH'`; `scripts/run_pig.sh` passes `HDFS_CLEAN_DIR` from the config, the same value Hive and Spark use | Tested (paths); Pig itself not run |
| H-1 | `.gitignore` rewritten: no more `hive/` or blanket `*.csv` ignore, duplicate blocks removed, patterns anchored; recorded Hive log and baseline logs are kept | Tested with `git check-ignore` |
| H-2 | Numeric regex now accepts exponents (`1.0E7`, `1e-5`); regex behaviour verified. `check_pipeline.sh` compares raw vs cleaned rows and warns if >1 % is dropped | Regex tested; row-count effect must be confirmed on the real data |
| H-3 | All scripts derive the repo root from their own location; `fraud_model.py` reads `PROJECT_HOME`/env; `run_model_debug.sh` (hard-coded paths, blocking `read`) removed; `test/pig_test.pig` uses a relative path | Tested incl. moving the repo after configuring |
| H-4 | `part-r-00000` check replaced by `_SUCCESS` checks everywhere (runner, `run_all.sh`, checker); incomplete outputs abort with the explicit `hdfs dfs -rm -r` command | Tested |
| M-1 | `PROJECT_HOME` consistently derived (scripts) and exported (`lib.sh`) | Tested |
| M-2 | `lib.sh` creates `logs/`; `start_services.sh` also checks HDFS safe mode | Syntax checked |
| M-3 | Stale failed `spark_run.log` deleted; runner writes a fresh log on the next run | Needs your run |
| M-4 | README/`spark/README.md` document the `setuptools` shim for Python 3.12 (no patch needed); wrong `artifacts/` reference removed | Done |
| M-5 | `install_dependencies.sh` installs `openjdk-8-jdk`, `python3-setuptools`, checks python3.12; README gives default install dirs and download locations for Hadoop/Pig/Hive/Spark (no automatic toolchain installer) | Syntax checked |
| M-6 | `configure_environment.sh` honours a user-supplied `SPARK_HOME` (falls back to defaults if invalid) | Tested |
| M-7 | Config accepts `HIVE_HOME` and tries `~/hive` and `~/apache-hive-4.0.1-bin`. The duplicate SLF4J jars and a Hive install path containing `%20` are machine-specific: relocate Hive to a path without spaces and delete one `slf4j` binding jar | Partly (environment) |
| M-8 | Config variables are now the single source for Pig (`-param`), Hive (`--hivevar`) and Spark (env) | Tested (stubs) |
| M-9 | `run_all.sh` rewritten: marker-based Hive skip (no grep for `7820`), `_SUCCESS` checks, upload only if needed (local CSV not required when HDFS has it), safe-mode check, stale-Hive marker removal after a re-clean, partial-output detection | Tested |
| M-10 | `pig -x local test/pig_test.pig`, relative path, `sample.csv` no longer ignored | Done |
| L-1 | Removed 17 backup files, 10 root `pig_*.log`, 3 `clean_paysim_manual_*` copies, `__pycache__`; Pig logs now go to `logs/` | Done |
| L-2 | `run_model_debug.sh` removed | Done |
| L-3 | `metrics.txt` is written only after predictions and model were saved; predictions are written first | Done (code) |
| L-4 | `verify_installation.sh` checks Java 8, setuptools, HDFS reachability and safe mode | Syntax/stub checked |

## Not changed
- `hive/fraud_analysis_output.log` still holds the output of the old cleaning (6,101,255 rows). It refreshes when Hive is re-run after re-cleaning.
- Modelling caveats in the audit (balance features tied to PaySim's simulator, `step` as a feature, random rather than time-based split) are documented, not altered.
- `test/sample.csv` and `LICENSE` unchanged.
