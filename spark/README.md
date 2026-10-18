# FraudLens — Part 3: Spark MLlib Random Forest

## Purpose
Train and evaluate a fraud classifier on the cleaned PaySim transactions produced by the Pig stage (the same data Hive analyses).

## Model
`RandomForestClassifier` (class-weighted) in a Spark ML pipeline:
`StringIndexer(type) → OneHotEncoder → VectorAssembler → RandomForestClassifier`.
Hyper-parameters come from `config/fraudlens.env` (`RF_NUM_TREES`, `RF_MAX_DEPTH`, `RF_MAX_BINS`,
`RF_SUBSAMPLING_RATE`, `RF_MIN_INSTANCES_PER_NODE`). Account IDs and `isFlaggedFraud` are not used as features.

## Evaluation
Accuracy, fraud precision/recall/F1, ROC-AUC, PR-AUC, test-set confusion matrix, precision/recall at probability thresholds
0.3 / 0.5 / 0.7 / 0.9, and ranked feature importances.

## Outputs
- HDFS model: `$HDFS_MODEL_DIR` (default `/fraudlens/models/spark_fraud_rf_model`)
- HDFS predictions (CSV with header): `$HDFS_PREDICTIONS_DIR` (default `/fraudlens/results/spark_fraud_rf_predictions`)
- Local metrics: `spark/results/metrics.txt` (written last, only after model and predictions were saved)
- Run log: `spark/results/spark_run.log`

## Run
`bash spark/run_fraud_model.sh` (after `source config/fraudlens.env` is not required; the script loads it itself).

The runner and the script refuse to overwrite existing model or prediction paths and require `$HDFS_CLEAN_DIR/_SUCCESS`.

## Requirements
Python 3.12 with `numpy`, `packaging` and `setuptools` (the latter provides the `distutils` shim PySpark 3.5 imports).
