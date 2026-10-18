# FraudLens — Big Data Fraud Detection on PaySim

End-to-end fraud-detection pipeline on the [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) mobile-money dataset:
**Hadoop (HDFS/YARN) → Pig (cleaning) → Hive (analytics) → Spark MLlib (Random Forest classifier).**

> **Status of this revision (6).** The full chain **Hadoop → Pig → Hive → Spark Random Forest was run successfully on a real
> Ubuntu 24.04 VM on a 100,000-row dataset** (`--sample-rows 100000`, see below). The full 6.36-million-row run uses the same
> scripts; follow [Running the full dataset](#running-the-full-dataset). Problems met on the first real run and their fixes are
> in [Troubleshooting: problems met on a real machine](#troubleshooting-problems-met-on-a-real-machine). The numbers in
> [Section 7](#7-results) are from older runs; refresh them from `spark/results/metrics.txt` after your own run.

---

## Quick start on a new machine (after `git clone`)

Tested target: Ubuntu 24.04 (or 22.04), 4 GB RAM or more, **at least 8 GB free disk** for setup plus a 100,000-row run (about 5 GB more for the full dataset), internet access for the first setup.

```bash
git clone https://github.com/<your-user>/<your-repo>.git
cd <your-repo>

bash setup/bootstrap.sh                 # packages, Hadoop/Pig/Hive/Spark, configs, NameNode format, verification
mkdir -p data/raw/paysim
cp /path/to/PS_20174392719_1491204439457_log.csv data/raw/paysim/     # not in git (470 MB)
bash scripts/run_all.sh                 # starts services if needed, then upload -> Pig -> Hive -> Spark
bash scripts/check_pipeline.sh          # read-only verification
```

What happens without any manual step:

- `config/fraudlens.env` is generated automatically when missing (`run_all.sh` calls `setup/configure_environment.sh`).
- HDFS, YARN and Hive are started automatically when they are down (`scripts/start_services.sh`), after checking SSH and safe mode.
- The CSV is found in `data/raw/paysim/`, `data/raw/` or `data/`, and its header is validated before the upload.
- Pig always runs with `-x mapreduce` against HDFS, with container sizes taken from the machine's RAM.
- Hadoop/YARN is configured for the machine's RAM (`setup/configure_hadoop.sh`), which prevents jobs freezing in `PREP`/`ACCEPTED`.

Already have Hadoop, Pig, Hive and Spark installed? Skip the downloads: `SKIP_TOOLCHAIN=1 bash setup/bootstrap.sh`
(put the tools in `~/hadoop`, `~/pig-0.17.0`, `~/hive`, `~/spark-3.5.9-bin-hadoop3`, or export `*_HOME` first).
`bootstrap.sh` never overwrites a hand-written Hadoop/Hive config: it keeps it and tells you; `--force` replaces it after making a backup.

## Running on a 100,000-row sample

For a quick, complete end-to-end run (a few minutes instead of hours) use a fixed-size sample of the CSV:

```bash
bash scripts/run_all.sh --sample-rows 100000 --no-prompt    # split -> Pig -> Hive -> Spark on exactly 100,000 rows
bash scripts/check_pipeline.sh                              # checks the sample run (same folders)
bash scripts/run_ui.sh --export                             # dashboard shows the sample results
```

- Exactly N rows are kept. `SAMPLE_MODE=random` (default) picks them evenly over the file with a fixed seed (`SAMPLE_SEED`, default 42),
  keeps the original time order and gives the same rows every time. `SAMPLE_MODE=head` takes the first N rows (few frauds: PaySim frauds are spread over time).
- Everything goes to separate HDFS folders ending in `_sample100000` and a separate Hive marker, so the full-data results are never touched.
  Hive recreates its external table for whichever folder is active (the table is only metadata; no data is deleted).
- A sample is one Pig job (one part). The next `run_all.sh` **without** `--sample-rows` is a full run again.
- Expect about 150 frauds in 100,000 rows, so about 30 in the 20 % test set: precision/recall are noisy at that size. Use the full data for final numbers.

## Running the full dataset

Do this after the 100,000-row run has worked. Everything below is run from the project folder.

**Before you start (each point caused a real problem):**

1. **The CSV must be on the local disk**, in `data/raw/paysim/`:
   ```bash
   unzip -o ~/Downloads/archive.zip -d data/raw/paysim        # Kaggle download "paysim1"
   ls -lh data/raw/paysim/                                     # PS_20174392719_1491204439457_log.csv, about 470 MB
   wc -l data/raw/paysim/PS_20174392719_1491204439457_log.csv  # must be 6362621 (6,362,620 rows + header)
   ```
   If the pipeline cannot find the local CSV it silently falls back to a file already in HDFS. After a sample run that file may hold
   only 100,000 rows, so a "full" run would use 100,000 rows. Check that the first lines of the run say
   `Mode: full dataset` and `Splitting the local file ... into parts of 600000 rows` (not `Splitting the HDFS file`).
2. **Disk space: have at least 5 GB free** (`df -h /`). The CSV, its parts in HDFS and the cleaned data all take space; a full disk
   makes HDFS enter safe mode and breaks the setup (see troubleshooting).
3. **Remove stale results from earlier runs** so stages are not skipped as "already complete":
   ```bash
   hdfs dfs -ls /fraudlens/raw /fraudlens/processed /fraudlens/models /fraudlens/results
   hdfs dfs -rm /fraudlens/raw/PS_20174392719_1491204439457_log.csv                  # an old 100k copy, if present
   hdfs dfs -rm -r -skipTrash /fraudlens/processed/paysim_cleaned /fraudlens/processed/_parts \
       /fraudlens/models/spark_fraud_rf_model /fraudlens/results/spark_fraud_rf_predictions   # old full-data results, if present
   rm -f logs/hive_analysis_completed
   ```
   (A `paysim_cleaned` folder from the very first version of the pipeline was cleaned with the old numeric filter that lost about 4 % of
   the rows; leaving it makes the runner skip Pig, Hive and Spark. The `*_sample100000` folders are not touched.)

**Run:**
```bash
bash scripts/start_services.sh          # HDFS, YARN, JobHistory, Hive metastore, HiveServer2
bash scripts/run_all.sh --no-prompt     # no --sample-rows = the whole dataset, 11 parts of 600,000 rows
```
Roughly 30 to 60 minutes for Pig (about 3 to 6 minutes per part on a small VM), about 10 minutes for Hive and 15 to 30 minutes for Spark.
You may stop at any time (Ctrl-C) and run the same command to resume; finished parts are skipped. To try it first:
`bash scripts/run_all.sh --max-parts 2` cleans two parts and analyses only those.

**Check:** `cat spark/results/metrics.txt`, `bash scripts/check_pipeline.sh` (cleaned rows about 6.36 million, under 1 % dropped),
`bash scripts/run_ui.sh --export` (dashboard). The dashboard and the checker follow the last run: a run without `--sample-rows` is the full data.

## Troubleshooting: problems met on a real machine

These all happened on the first real run (VirtualBox, Ubuntu 24.04, 25 GB disk). Symptom, cause, fix.

| Symptom | Cause | Fix |
|---|---|---|
| `mkdir: cannot create directory ... No space left on device` during setup; the toolchain install stops half-way | The VM disk is full. The setup needs about 6 GB free | Free space (below), or enlarge the VM disk, then rerun `bash setup/bootstrap.sh` |
| `ln: failed to create symbolic link '/home/<you>/hive': File exists` and Spark is never installed | A stale or broken `~/hive` link (often left by an interrupted install) aborted the script before Spark was downloaded | `rm -f ~/hive`, then `bash setup/install_toolchain.sh` (fixed in revision 6: `ln -sfn`) |
| `configure_environment.sh` reports `Spark: .../spark-2.4.8-bin-hadoop2.7` | `SPARK_HOME` in your shell or `~/.bashrc` pointed at an old Spark, and the config script prefers that variable. Spark 2.4 cannot run this pipeline | `unset SPARK_HOME HIVE_HOME`, remove those lines from `~/.bashrc`, `rm config/fraudlens.env`, `bash setup/configure_environment.sh`; it must now show `spark-3.5.9-bin-hadoop3` |
| `start_services.sh` hangs at `Checking YARN` (blank lines), `YARN not responding` | The Hadoop config had been written while the disk was full | `$HADOOP_HOME/sbin/stop-yarn.sh; $HADOOP_HOME/sbin/stop-dfs.sh`, `bash setup/configure_hadoop.sh --force`, `bash scripts/start_services.sh`; look at `$HADOOP_HOME/logs/*nodemanager*.log` and `bash scripts/diagnose.sh` if it persists |
| `ERROR: Timed out waiting for the Hive metastore (port 9083)` | The Derby metastore was never initialised (or was half-created while the disk was full) | `pkill -f "hive --service"; rm -rf ~/hadoop_data/hive_metastore_db ~/hadoop_data/derby.log; bash setup/configure_hive.sh --force; bash scripts/start_services.sh`. Only Hive's catalogue is removed, not HDFS data |
| Pig prints `Retrying connect to server: 0.0.0.0/0.0.0.0:10020` for minutes | The MapReduce JobHistory server is not running; clients ask it for the result of a finished job | `$HADOOP_HOME/bin/mapred --daemon start historyserver` (revision 6 starts it in `start_services.sh`). The Pig job may already have finished: look for `_SUCCESS` in `/fraudlens/processed/_parts*/part_0001` and rerun `run_all.sh`; finished parts are skipped |
| Hive output looks like it repeats | Beeline echoes each query several times and every query is two MapReduce jobs of about 1 to 1.5 minutes | Wait. Job ids (`job_..._0006`, `_0007`, ...) and the `Stage-1 map = x%` lines change; it is not looping. 6 queries take about 8 minutes |
| `cat: Unable to write to output stream.` while splitting | Harmless: `head` closes the pipe after reading the CSV header | Ignore |
| `Splitting the HDFS file ...` and `the file has only 100000 data rows` | The local CSV was not found; an older 100,000-row file in `/fraudlens/raw/` was used | Put the full CSV in `data/raw/paysim/` and delete the old HDFS copy (see "Running the full dataset") |
| Spark stage fails at import with `No module named 'pandas'` | Spark 3.5 `pyspark.ml.functions` needs pandas; the setup does not install it | Fixed in revision 6 (fallback in `fraud_model.py`). Alternatively `python3.12 -m pip install --break-system-packages pandas` |
| `check_pipeline.sh` shows `cleaned rows=0` | Old versions looked for `part-*` but part-mode files are named `pNNNN-part-r-*` | Fixed in revision 6 |
| A stage is skipped as "already complete" when you wanted a fresh run | Output with `_SUCCESS` exists in HDFS (or `logs/hive_analysis_completed`) | Remove that output deliberately (commands under "Running the full dataset"); nothing is ever deleted automatically |

**Freeing disk space safely** (do this before setup or a full run; check with `df -h /`):
```bash
rm -rf ~/Downloads/fraudlens          # downloaded tool archives (about 1.5 GB), not needed after install
rm -f ~/Downloads/archive.zip         # only after data/raw/paysim/ has the extracted CSV
sudo apt-get clean && sudo apt-get autoremove -y
sudo journalctl --vacuum-size=50M
rm -rf ~/.cache/* ; sudo rm -rf /tmp/*
hdfs dfs -rm -r -skipTrash /fraudlens/raw/parts_sample100000 /fraudlens/processed/_parts_sample100000   # sample leftovers, optional
```
To enlarge a VirtualBox disk: shut the VM down, File, Tools, Virtual Media Manager, resize the disk, boot, then
`sudo apt install -y cloud-guest-utils; sudo growpart /dev/sda 2; sudo resize2fs /dev/sda2` (check the partition number with `lsblk`;
with LVM use `sudo lvextend -r -l +100%FREE <lv path>`).

**Order that works on a fresh VM:** free 8 GB or more, `bash setup/bootstrap.sh`, check `config/fraudlens.env` names Spark 3.5.9,
`bash scripts/start_services.sh`, `bash scripts/run_all.sh --sample-rows 100000 --no-prompt`, then the full run.

## Running in parts (stop and resume)

The Pig cleaning is the slow, resumable part of the pipeline, so it works on the data in **parts of 600,000 rows**
(PaySim has about 6.36 million rows, so about 11 parts). After every part you can stop and carry on with what is done.

```bash
bash scripts/run_all.sh                  # asks after each part (continues by itself after 20 s)
bash scripts/run_all.sh --max-parts 3    # clean 3 new parts, then analyse what is done
bash scripts/run_all.sh --no-prompt      # never ask, run everything
bash scripts/run_all.sh --rows 300000    # different part size (0 = one file, the old behaviour)
```

After each part the runner prints `>>> 3 of 11 parts are done.` and offers:

| Key | What happens |
|---|---|
| Enter (or wait 20 s) | continue with the next part |
| `a` | stop here and **continue with what is done**: Hive and Spark run on the parts cleaned so far |
| `q` | stop here, run nothing else, resume later |

Other ways to stop: `touch logs/STOP` from a second terminal (same as `a`, after the current part), or Ctrl-C at any time
(the part being cleaned is discarded, finished parts are kept). **To resume, run the same command again**: finished parts are
skipped and the rest are cleaned.

```
raw CSV --split--> HDFS /fraudlens/raw/parts/part_0001.csv ... part_0011.csv      (streamed, no extra local copy)
   each part --Pig--> HDFS /fraudlens/processed/_parts/part_0003/   (_SUCCESS = this part is finished)
                  --> files moved to /fraudlens/processed/paysim_cleaned/p0003-part-r-*   (what Hive and Spark read)
```

Things to know:

- **Hive and Spark are not incremental.** They always run once over every part that is in `paysim_cleaned`. When you add more
  parts later, Hive re-runs automatically; for Spark use `bash scripts/run_all.sh --retrain` (it deletes the old model and
  predictions and trains again). Without `--retrain` the old model is kept and a note is printed.
- **The parts follow the time order of the file** (PaySim is sorted by `step`), so a partial run covers only the first hours of the
  simulation. The dashboard shows a "Partial data" banner and a "Data parts cleaned" line in Pipeline status while parts are missing.
- **Duplicates are removed inside each part only** (`DISTINCT` runs per part). An identical row appearing in two different parts
  would be kept; this was not measured on the real data.
- Each part is a separate Pig job, so the total time is a little higher than one big job (job start-up is paid per part). The
  gain is that nothing is lost when you stop, and you get usable results after the first parts.
- A crashed or interrupted part leaves an unfinished folder under `_parts/`; the next run removes it and redoes that part.
  An interrupted split is also redone (it is only a copy of the CSV).
- Data from an earlier single-file run (`paysim_cleaned` with `_SUCCESS`, no `_parts/`) is detected and left alone.

## Dashboard (optional, for demos)

A small read-only web dashboard that shows what the pipeline actually produced. It adds no new processing: it reads the
Hive log, `spark/results/metrics.txt` and the Spark predictions, and says "not available yet" for anything that has not run.
It is not needed to run the pipeline.

```bash
bash scripts/run_all.sh                 # produce the results first
bash scripts/run_ui.sh --export         # copy the predictions out of HDFS, then start the dashboard
# open http://127.0.0.1:8501
```

| Tab | Shows | Source |
|---|---|---|
| Overview | dataset size, fraud rate, fraud by transaction type, fraud per simulated day | Hive log, predictions |
| Transactions | search and filter the test-set predictions (type, status, amount, risk score), with caught / false alarm / missed labels | predictions |
| Fraud alerts | highest-risk transactions at a chosen threshold, and how many are real fraud | predictions |
| Model performance | accuracy, precision, recall, F1, ROC-AUC, PR-AUC, confusion matrix, feature importances, interactive threshold explorer with a precision-recall curve | `metrics.txt`, predictions |
| Pipeline status | HDFS/Pig/Hive/Spark stages with their output locations and timestamps | HDFS `_SUCCESS` markers, local files |

The first start creates `.venv-ui` and installs Flask, pandas and numpy (internet needed once). The threshold explorer recomputes
precision and recall from the real predicted probabilities, so the numbers match `metrics.txt` at 0.5. The app listens on
localhost only. Port: `UI_PORT=9000 bash scripts/run_ui.sh`.

Suggested video order: architecture, `bash scripts/run_all.sh` (already-complete stages are skipped), the dashboard tabs
(Overview, Model performance with the threshold slider, Fraud alerts, Pipeline status), `bash scripts/check_pipeline.sh`.

## Publishing to GitHub

```bash
cd bigdata_project
git init -b main
git add .
git update-index --chmod=+x scripts/*.sh setup/*.sh spark/*.sh      # keep scripts executable on other machines
git status                                                          # data/, config/fraudlens.env, logs/, models are ignored
git commit -m "FraudLens: Hadoop + Pig + Hive + Spark fraud pipeline"
git remote add origin https://github.com/<your-user>/<your-repo>.git
git push -u origin main
```

Not committed on purpose (see `.gitignore`): the dataset, `config/fraudlens.env` (machine specific, regenerated by
`bootstrap.sh`), `logs/`, downloaded tools and generated models. `.gitattributes` forces LF line endings so the scripts
also work after a checkout on Windows/WSL.

---

## 1. Pipeline

```
PaySim CSV (local: data/raw/paysim/)
   │  scripts/prepare_data.sh
   ▼
HDFS  /fraudlens/raw/PS_20174392719_1491204439457_log.csv
   │  Part 1  scripts/run_pig.sh → pig/clean_paysim.pig   (validate, cast, balance deltas, DISTINCT)
   ▼
HDFS  /fraudlens/processed/paysim_cleaned     (13 comma-separated columns, no header, _SUCCESS marker)
   ├─ Part 2  scripts/run_hive.sh → hive/fraud_analysis.hql  → hive/fraud_analysis_output.log
   └─ Part 3  spark/run_fraud_model.sh → spark/fraud_model.py (Random Forest)
                 → HDFS /fraudlens/models/spark_fraud_rf_model
                 → HDFS /fraudlens/results/spark_fraud_rf_predictions
                 → spark/results/metrics.txt
```

Every HDFS path comes from one file, `config/fraudlens.env`, and is handed to Pig (`-param`), Hive (`--hivevar`) and
Spark (environment variables), so the three stages cannot drift apart.

## 2. Repository layout

| Path | Purpose |
|---|---|
| `setup/bootstrap.sh` | One-shot setup of a new machine (runs the four scripts below plus configure/verify) |
| `setup/install_toolchain.sh` | Downloads Hadoop 3.3.6, Pig 0.17.0, Hive 4.0.1, Spark 3.5.9 into `$HOME` (skips what exists) |
| `setup/configure_hadoop.sh` | Writes RAM-sized HDFS/YARN/MapReduce config, sets up SSH, formats the NameNode once |
| `setup/configure_hive.sh` | Writes `hive-site.xml` (fixed Derby path, MapReduce engine) and initialises the metastore |
| `setup/install_dependencies.sh` | apt packages: OpenJDK 8, Python helpers (numpy, packaging, setuptools), SSH. Ubuntu only |
| `setup/configure_environment.sh` | Detects installed tools, writes `config/fraudlens.env` |
| `setup/verify_installation.sh` | Checks executables, Java 8, Python deps, Spark version, HDFS reachability |
| `scripts/start_services.sh` | Starts HDFS, YARN, Hive metastore, HiveServer2; aborts if HDFS is in safe mode |
| `scripts/prepare_data.sh` | Uploads the raw CSV to HDFS (never overwrites); used when `ROWS_PER_PART=0` |
| `scripts/split_data.sh` | Splits the CSV into parts of `ROWS_PER_PART` rows (default 600000) straight into HDFS |
| `scripts/run_pig_parts.sh` | Cleans the parts one by one with a stop point after each; see "Running in parts" |
| `scripts/export_results.sh` · `scripts/run_ui.sh` | Export predictions from HDFS; start the optional dashboard (`ui/`) |
| `ui/` | Flask dashboard (`app.py`, `static/index.html`, `requirements.txt`); `ui/data/` holds the exported predictions (git-ignored) |
| `scripts/diagnose.sh` | Read-only snapshot (memory, daemons, YARN nodes/apps, memory settings) for hung jobs |
| `scripts/run_pig.sh` · `run_hive.sh` | Stage wrappers (parameters from the config); `run_pig.sh [<hdfs_in> <hdfs_out>]` cleans one input file |
| `scripts/run_all.sh` | Master runner; skips stages whose output is complete (`_SUCCESS`); options `--max-parts`, `--no-prompt`, `--rows`, `--retrain` |
| `scripts/check_pipeline.sh` | Read-only configuration and pipeline checker |
| `scripts/lib.sh` | Shared helpers (config loading, `hdfs_uri`, `hdfs_job_done`) |
| `pig/clean_paysim.pig` | Part 1 cleaning (parameters `RAW_PATH`, `OUT_PATH`) |
| `hive/fraud_analysis.hql` | Part 2 analytics (variable `HDFS_CLEAN_DIR`); `hive/fraud_analysis_output.log` = captured output |
| `spark/fraud_model.py`, `spark/run_fraud_model.sh` | Part 3 Random Forest training/evaluation and runner |
| `test/` | Pig smoke test (`pig -x local test/pig_test.pig`) + 2-row sample |
| `docs/` | `AUDIT_REPORT.md` (original findings) and `baseline_logreg/` (previous logistic-regression run) |
| `config/fraudlens.env` | **Generated**, machine-specific, git-ignored |

The repository can live in any directory: scripts locate themselves instead of assuming `~/FraudLens`.

## 3. Environment

| Component | Version |
|---|---|
| OS | Ubuntu 24.04 (Python 3.12 is the system Python) |
| Java | OpenJDK 8 |
| Hadoop | 3.3.6, pseudo-distributed, `fs.defaultFS = hdfs://localhost:9000` |
| Pig | 0.17.0 (MapReduce mode) |
| Hive | 4.0.1 (HiveServer2 + Beeline) |
| Spark | 3.5.9, `local[2]`, 2 GB driver (configurable) |
| Python | 3.12 with `numpy`, `packaging`, `setuptools` |

`setuptools` matters: Python 3.12 removed `distutils`, which PySpark 3.5 still imports (`LooseVersion`). Installing
`python3-setuptools` (done by `install_dependencies.sh`) supplies the shim; no patching of Spark is required.

### Installing the toolchain
`install_dependencies.sh` installs Java 8 and the Python packages. Hadoop, Pig, Hive and Spark are unpacked by hand into the
default locations the configure script looks for (or set `HADOOP_HOME`, `PIG_HOME`, `HIVE_HOME`, `SPARK_HOME` first):

| Tool | Default directory | Download |
|---|---|---|
| Hadoop 3.3.6 | `~/hadoop` | `archive.apache.org/dist/hadoop/common/hadoop-3.3.6/` |
| Pig 0.17.0 | `~/pig-0.17.0` | `archive.apache.org/dist/pig/pig-0.17.0/` |
| Hive 4.0.1 | `~/hive` | `archive.apache.org/dist/hive/hive-4.0.1/` |
| Spark 3.5.9 | `~/spark-3.5.9-bin-hadoop3` | `archive.apache.org/dist/spark/spark-3.5.9/` |

Configure Hadoop for pseudo-distributed mode (`core-site.xml`: `fs.defaultFS=hdfs://localhost:9000`; `hdfs-site.xml`; passwordless
SSH to localhost; `hdfs namenode -format` once) using the official single-node guide. Keep free disk space: the NameNode enters
safe mode when it runs low, which broke several earlier runs. Small VMs (about 4 GB RAM) are supported: container sizes
are derived from the RAM, for example NodeManager 2949 MB and 768 MB containers on a 4.8 GB machine.

## 4. Configuration

```bash
bash setup/install_dependencies.sh
# unpack Hadoop/Pig/Hive/Spark (see above)
bash setup/configure_environment.sh        # writes config/fraudlens.env
source config/fraudlens.env
bash setup/verify_installation.sh
```

Key settings written to `config/fraudlens.env` (override by exporting the variable before running the configure script):

```
HDFS_RAW_DIR=/fraudlens/raw                      HDFS_CLEAN_DIR=/fraudlens/processed/paysim_cleaned
HDFS_MODEL_DIR=/fraudlens/models/spark_fraud_rf_model
HDFS_PREDICTIONS_DIR=/fraudlens/results/spark_fraud_rf_predictions
DATASET_FILENAME=PS_20174392719_1491204439457_log.csv
SPARK_MASTER=local[2]  SPARK_DRIVER_MEMORY=2g  SPARK_SHUFFLE_PARTITIONS=8
RF_NUM_TREES=40  RF_MAX_DEPTH=8  RF_MAX_BINS=32  RF_SUBSAMPLING_RATE=0.5  RF_MIN_INSTANCES_PER_NODE=10
HIVE_JDBC_URL=jdbc:hive2://localhost:10000/default
```

If you already have a `config/fraudlens.env` from the previous version, **regenerate it**: it lacks the `RF_*` settings and still
names the old logistic-regression output paths.

## 5. Running

```bash
mkdir -p data/raw/paysim && cp PS_20174392719_1491204439457_log.csv data/raw/paysim/
bash scripts/start_services.sh
bash scripts/run_all.sh                    # upload → Pig → Hive → Spark
```

Single stages: `scripts/prepare_data.sh`, `scripts/run_pig.sh`, `scripts/run_hive.sh`, `spark/run_fraud_model.sh`.

Nothing in HDFS is ever deleted or overwritten by the scripts. If an output already exists they either skip (complete, `_SUCCESS`
present) or stop and print the exact `hdfs dfs -rm -r ...` command to run if you really want to redo it. Re-cleaning with Pig makes
the Hive result and the Spark model stale; remove those too (`rm logs/hive_analysis_completed`, delete model/predictions dirs).

**Moving from the old pipeline:** the cleaned data in `/fraudlens/processed/paysim_cleaned` was produced by the old numeric filter,
which dropped rows containing scientific-notation values. To benefit from the fix, remove that directory and rerun:
`hdfs dfs -rm -r /fraudlens/processed/paysim_cleaned && rm -f logs/hive_analysis_completed && bash scripts/run_all.sh`.
The Hive table is external, so it reads the new data automatically.

## 6. Checking configuration and pipeline

```bash
source config/fraudlens.env
bash scripts/check_pipeline.sh
```

The checker is read-only and reports PASS/WARN/FAIL for:

1. **Configuration** — all variables set, Java 8, Python dependencies (numpy, packaging, setuptools).
2. **Cross-file consistency** — Pig/Hive/Spark take their paths from the config (no literal HDFS paths), no hard-coded home
   directories, Hive script and test data not git-ignored.
3. **Services** — HDFS reachable, safe mode off, YARN, HiveServer2 on port 10000.
4. **Artefacts** — raw data in HDFS, Pig output has `_SUCCESS` and 13 columns, rows dropped by cleaning (warns above 1 %),
   Random Forest model and predictions present, `metrics.txt` produced by the Random Forest.

Manual spot checks:

```bash
hdfs dfs -ls "$HDFS_CLEAN_DIR"                                   # _SUCCESS + part-* files
beeline -u "$HIVE_JDBC_URL" -e "SELECT COUNT(*), SUM(isFraud) FROM fraudlens.paysim_cleaned"
cat spark/results/metrics.txt
```

Expected after the filter fix (stock PaySim): **6,362,620 rows, 8,213 fraud, 16 flagged** if nothing legitimate is dropped.
This is the expectation to confirm on the first run, not a measured result.

### Troubleshooting

| Symptom | Cause / action |
|---|---|
| `config/fraudlens.env` missing | Generated automatically on the next `run_all.sh`; or `bash setup/configure_environment.sh` |
| `Failed to create DataStorage`, connection refused to `localhost:9000` | HDFS/YARN not running. `run_all.sh` starts them; manually `bash scripts/start_services.sh` |
| YARN job stuck in `PREP` / `ACCEPTED` on a small VM | Containers larger than free NodeManager memory, or the ApplicationMaster share too low. `bash setup/configure_hadoop.sh --force`, restart YARN, lower `PIG_TASK_MB` (for example 768); inspect with `bash scripts/diagnose.sh`; kill with `yarn application -kill <id>` |
| `... not found` for the CSV | Put it in `data/raw/paysim/` (also found in `data/raw/` or `data/`), or set `DATASET_LOCAL_PATH` in `config/fraudlens.env` |
| `Incomplete HDFS URI, no host` (Pig on `file:///`) | Pig ran in local mode. `run_pig.sh` now passes `-x mapreduce` and puts the Hadoop config on `PIG_CLASSPATH` |
| `pig -x mapreduce: No such file or directory` | The option got glued into the executable path. Options must be separate words; `run_pig.sh` uses a command array |
| `Failed to create DataStorage` after a reboot | Same as the first services row: daemons are not persistent across reboots |
| `UnsetHostnameOrPort` | `fs.defaultFS` missing; `bash setup/configure_hadoop.sh` |
| `SafeModeException ... Resources are low on NN` | Free disk space, then `hdfs dfsadmin -safemode leave` |
| `Output directory ... already exists` | Intended guard; remove it deliberately (the error prints the command) or skip the stage |
| Hive tables vanished after starting Hive from another directory | Derby metastore was relative to the current directory; `setup/configure_hive.sh` pins it to `~/hadoop_data/hive_metastore_db` |

## 7. Results

### Random Forest (current model, full PaySim data)
Measured on the full dataset after the fixed pipeline (`spark/results/metrics.txt`, run on the Ubuntu VM):

| Item | Value |
|---|---:|
| Raw rows (without header) | 6,362,620 |
| Rows after Pig cleaning | 6,362,620 (0 dropped) |
| Fraud rows | 8,213 (0.129 %) |
| Training / test rows | 5,089,383 / 1,273,237 (80/20, seed 42) |

| Metric (test set, threshold 0.5) | Value |
|---|---:|
| Accuracy | 97.93 % |
| Fraud precision | 5.83 % |
| Fraud recall | 98.97 % |
| Fraud F1 | 0.1102 |
| ROC-AUC | 0.9985 |
| PR-AUC | 0.8557 |

Confusion matrix: TN 1,245,214 · FP 26,372 · FN 17 · TP 1,634 (1,651 frauds in the test set, 1,634 caught).

Precision and recall at other probability thresholds (the dashboard has an interactive version):

| Threshold | Precision | Recall | Transactions flagged | TP | FP |
|---:|---:|---:|---:|---:|---:|
| 0.3 | 3.75 % | 99.39 % | 43,793 | 1,641 | 42,152 |
| 0.5 | 5.83 % | 98.97 % | 28,006 | 1,634 | 26,372 |
| 0.7 | 11.25 % | 96.97 % | 14,236 | 1,601 | 12,635 |
| 0.9 | 44.48 % | 86.07 % | 3,195 | 1,421 | 1,774 |

Feature importances (highest first): `originBalanceChange` 0.348, `oldbalanceOrg` 0.163, `newbalanceOrig` 0.115, `amount` 0.099,
`type=PAYMENT` 0.056, `destBalanceChange` 0.047, `type=TRANSFER` 0.042, `type=CASH_IN` 0.035, `step` 0.030, `newbalanceDest` 0.030,
`oldbalanceDest` 0.023, `type=CASH_OUT` 0.011, `type=DEBIT` 0.0002. The derived balance-change feature built in Pig is the strongest one.

How to read it: the model catches almost every fraud (recall 98.97 %) but, because fraud is only 0.13 % of the rows and the classes are weighted
to favour recall, it raises many false alarms (precision 5.83 %). Raising the threshold to 0.9 gives precision 44.5 % at 86.1 % recall.
Accuracy is not meaningful at a 0.13 % fraud rate; judge the model on PR-AUC, recall and precision at a chosen threshold.

Model: `RandomForestClassifier` in a pipeline `StringIndexer → OneHotEncoder → VectorAssembler → RandomForest`, 40 trees, depth 8,
32 bins, 50 % row subsampling per tree, inverse-frequency class weights, seed 42, 80/20 random split. No scaler is used
(trees are scale-invariant). Features: `step`, `amount`, origin/destination old and new balances, the two derived balance
changes, and the one-hot transaction type. Account IDs and `isFlaggedFraud` are excluded. The Hive analysis of the same data is in
`hive/fraud_analysis_output.log`.

### Previous baseline: weighted Logistic Regression (old cleaning, 6,101,255 rows)
Kept in `docs/baseline_logreg/` for comparison only. It used the old numeric filter that dropped 261,365 rows, so it is not a like-for-like
comparison; the Random Forest above improves PR-AUC from 0.300 to 0.856 and ROC-AUC from 0.962 to 0.999.

| Metric | Value |
|---|---:|
| Accuracy | 86.07 % |
| Fraud precision | 0.83 % |
| Fraud recall | 90.48 % |
| Fraud F1 | 1.65 % |
| ROC-AUC | 0.9622 |
| PR-AUC | 0.3000 |

Confusion matrix: TN 1,049,613 · FP 169,917 · FN 150 · TP 1,425. Accuracy is not meaningful at a 0.13 % fraud rate; compare
models on PR-AUC and on precision at a fixed recall.

## 8. Known limitations

- `oldbalance*/newbalance*` are tightly tied to how PaySim simulates fraud (almost every fraud row ends with `newbalanceOrig = 0`;
  Hive query 6 in `hive/fraud_analysis_output.log` gives the exact count), so the very high recall and ROC-AUC will not transfer to real transaction data.
- `step` is a time counter; consider dropping it or using a time-based split instead of a random split.
- A 40-tree forest on ~5 M training rows with `local[2]` and 2 GB takes a while; lower `RF_NUM_TREES`/`RF_MAX_DEPTH` or raise
  `SPARK_DRIVER_MEMORY` in `config/fraudlens.env` to trade accuracy for speed.
- Pig 0.17.0 targets Hadoop 2.x. The first recorded Pig failure (`Job in state DEFINE instead of RUNNING`) may come from that combination with Hadoop 3.3.6; later runs succeeded, but keep it in mind if it reappears.

## 9. License

MIT — see `LICENSE`.
