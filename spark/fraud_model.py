"""FraudLens Part 3: fraud classification with a Spark MLlib Random Forest.

All paths and hyper-parameters come from environment variables exported by
config/fraudlens.env (see spark/run_fraud_model.sh); the defaults below match that file.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)
from pyspark.storagelevel import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
try:
    from pyspark.ml.functions import vector_to_array
except ImportError:
    # pyspark.ml.functions imports pandas at module load (Spark 3.5). When pandas is not installed for the Python that runs
    # Spark, fall back to an equivalent that needs nothing extra: an ML vector -> array<double> UDF.
    from pyspark.sql.types import ArrayType

    def vector_to_array(col):
        return F.udf(lambda v: [float(x) for x in v.toArray()] if v is not None else None,
                     ArrayType(DoubleType()))(col)


def qualify(path):
    """/a/b -> hdfs:///a/b ; paths that already carry a scheme are left alone."""
    return path if "://" in path else "hdfs://" + path


HDFS_CLEAN_DIR = os.environ.get("HDFS_CLEAN_DIR", "/fraudlens/processed/paysim_cleaned")
HDFS_MODEL_DIR = os.environ.get("HDFS_MODEL_DIR", "/fraudlens/models/spark_fraud_rf_model")
HDFS_PREDICTIONS_DIR = os.environ.get(
    "HDFS_PREDICTIONS_DIR", "/fraudlens/results/spark_fraud_rf_predictions")

INPUT_PATH = qualify(HDFS_CLEAN_DIR)
MODEL_PATH = qualify(HDFS_MODEL_DIR)
PREDICTIONS_PATH = qualify(HDFS_PREDICTIONS_DIR)

PROJECT_HOME = os.environ.get(
    "PROJECT_HOME", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
LOCAL_RESULTS = os.path.join(PROJECT_HOME, "spark", "results", "metrics.txt")

RF_NUM_TREES = int(os.environ.get("RF_NUM_TREES", "40"))
RF_MAX_DEPTH = int(os.environ.get("RF_MAX_DEPTH", "8"))
RF_MAX_BINS = int(os.environ.get("RF_MAX_BINS", "32"))
RF_SUBSAMPLING_RATE = float(os.environ.get("RF_SUBSAMPLING_RATE", "0.5"))
RF_MIN_INSTANCES = int(os.environ.get("RF_MIN_INSTANCES_PER_NODE", "10"))
SEED = 42
THRESHOLDS = [0.3, 0.5, 0.7, 0.9]

schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("originBalanceChange", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("destBalanceChange", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])

spark = SparkSession.builder.appName("FraudLens-SparkMLlib-RandomForest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def require_new_hdfs_path(path):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(hpath):
        raise RuntimeError("Output already exists; refusing to overwrite: " + path)


data = None
predictions = None
try:
    print("\n=== FRAUDLENS PART 3: SPARK MLlib RANDOM FOREST ===")
    print("Reading cleaned PaySim data from:", INPUT_PATH)

    require_new_hdfs_path(PREDICTIONS_PATH)
    require_new_hdfs_path(MODEL_PATH)

    raw = (
        spark.read
        .option("header", "false")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(INPUT_PATH)
    )

    # Account IDs and the pre-existing isFlaggedFraud indicator are excluded: IDs are
    # high-cardinality, and the flag would make the model depend on the dataset's own rule.
    data = (
        raw.select(
            "step", "type", "amount",
            "oldbalanceOrg", "newbalanceOrig", "originBalanceChange",
            "oldbalanceDest", "newbalanceDest", "destBalanceChange", "isFraud",
        )
        .dropna()
        .filter(F.col("isFraud").isin(0, 1))
        .withColumn("label", F.col("isFraud").cast("double"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total = data.count()
    print("Valid records used:", total)
    if total == 0:
        raise RuntimeError("No valid records were loaded. Check the HDFS input.")

    fraud_total = data.filter(F.col("label") == 1.0).count()
    legit_total = total - fraud_total
    fraud_rate = 100.0 * fraud_total / total
    print("Legitimate records:", legit_total)
    print("Fraud records:", fraud_total)
    print("Fraud percentage: %.5f%%" % fraud_rate)
    if fraud_total == 0 or legit_total == 0:
        raise RuntimeError("Both fraud and legitimate classes are required.")

    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    train_count = train.count()
    test_count = test.count()
    train_fraud = train.filter(F.col("label") == 1.0).count()
    train_legit = train_count - train_fraud
    if train_fraud == 0 or train_legit == 0 or test_count == 0:
        raise RuntimeError("Train/test split does not contain the required classes.")

    # Inverse-frequency weights so the forest does not ignore the rare fraud class.
    fraud_weight = float(train_count) / (2.0 * train_fraud)
    legit_weight = float(train_count) / (2.0 * train_legit)
    train = train.withColumn(
        "classWeight",
        F.when(F.col("label") == 1.0, F.lit(fraud_weight)).otherwise(F.lit(legit_weight)),
    )

    print("\nTraining records:", train_count)
    print("Testing records:", test_count)
    print("Class weights: fraud=%.3f legit=%.3f" % (fraud_weight, legit_weight))

    type_indexer = StringIndexer(inputCol="type", outputCol="typeIndex", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["typeIndex"], outputCols=["typeVector"], handleInvalid="keep")

    numeric_features = [
        "step", "amount",
        "oldbalanceOrg", "newbalanceOrig", "originBalanceChange",
        "oldbalanceDest", "newbalanceDest", "destBalanceChange",
    ]
    assembler = VectorAssembler(
        inputCols=numeric_features + ["typeVector"],
        outputCol="features",
        handleInvalid="skip",
    )

    # Tree models are scale-invariant, so no StandardScaler stage is needed.
    classifier = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        weightCol="classWeight",
        numTrees=RF_NUM_TREES,
        maxDepth=RF_MAX_DEPTH,
        maxBins=RF_MAX_BINS,
        subsamplingRate=RF_SUBSAMPLING_RATE,
        minInstancesPerNode=RF_MIN_INSTANCES,
        seed=SEED,
    )

    pipeline = Pipeline(stages=[type_indexer, encoder, assembler, classifier])

    print("\nTraining Random Forest (trees=%d, depth=%d, bins=%d, subsample=%.2f)..." % (
        RF_NUM_TREES, RF_MAX_DEPTH, RF_MAX_BINS, RF_SUBSAMPLING_RATE))
    fitted_model = pipeline.fit(train)
    print("Model training completed.")

    print("Generating test-set predictions...")
    predictions = fitted_model.transform(test).persist(StorageLevel.MEMORY_AND_DISK)
    predictions.count()

    auc_roc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(predictions)
    auc_pr = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    ).evaluate(predictions)

    # Confusion matrix at the model's default decision rule (majority vote of the trees).
    confusion = {
        (int(r["label"]), int(r["prediction"])): int(r["count"])
        for r in predictions.groupBy("label", "prediction").count().collect()
    }
    tn = confusion.get((0, 0), 0)
    fp = confusion.get((0, 1), 0)
    fn = confusion.get((1, 0), 0)
    tp = confusion.get((1, 1), 0)

    accuracy = float(tp + tn) / max(1, tp + tn + fp + fn)
    precision = float(tp) / max(1, tp + fp)
    recall = float(tp) / max(1, tp + fn)
    f1 = 2.0 * precision * recall / (precision + recall) if precision + recall > 0 else 0.0

    # Precision/recall trade-off at several probability thresholds.
    scored = predictions.select("label", vector_to_array("probability")[1].alias("p"))
    aggs = []
    for i, t in enumerate(THRESHOLDS):
        flagged = F.col("p") >= t
        aggs.append(F.sum(F.when(flagged & (F.col("label") == 1.0), 1).otherwise(0)).alias("tp%d" % i))
        aggs.append(F.sum(F.when(flagged & (F.col("label") == 0.0), 1).otherwise(0)).alias("fp%d" % i))
    row = scored.agg(*aggs).first()
    threshold_lines = []
    positives = tp + fn
    for i, t in enumerate(THRESHOLDS):
        t_tp = int(row["tp%d" % i] or 0)
        t_fp = int(row["fp%d" % i] or 0)
        t_prec = float(t_tp) / max(1, t_tp + t_fp)
        t_rec = float(t_tp) / max(1, positives)
        threshold_lines.append(
            "threshold %.1f: precision=%.6f recall=%.6f flagged=%d (TP=%d FP=%d)" % (
                t, t_prec, t_rec, t_tp + t_fp, t_tp, t_fp))

    # Feature importances, labelled with the one-hot transaction types.
    rf_model = fitted_model.stages[-1]
    type_labels = list(fitted_model.stages[0].labelsArray[0])
    feature_names = numeric_features + ["type=" + lbl for lbl in type_labels]
    importances = list(rf_model.featureImportances.toArray())
    # OneHotEncoder(handleInvalid="keep") reserves one extra slot for transaction types not seen in training.
    if len(importances) == len(feature_names) + 1:
        feature_names.append("type=<unseen>")
    if len(importances) != len(feature_names):
        feature_names = ["feature_%d" % i for i in range(len(importances))]
    ranked = sorted(zip(feature_names, importances), key=lambda kv: -kv[1])
    importance_lines = ["%-22s %.6f" % (name, value) for name, value in ranked]

    # Write the prediction CSV and the model first; the metrics file is written last so it
    # never claims outputs that do not exist.
    probability_of_fraud = vector_to_array(F.col("probability"))[1]
    (
        predictions.select(
            "step", "type", "amount", "isFraud",
            F.col("prediction").cast("integer").alias("predictedFraud"),
            probability_of_fraud.alias("fraudProbability"),
        )
        .write.mode("errorifexists").option("header", "true").csv(PREDICTIONS_PATH)
    )
    print("Predictions saved to:", PREDICTIONS_PATH)

    fitted_model.write().save(MODEL_PATH)
    print("Model saved to:", MODEL_PATH)

    metrics = [
        "FraudLens - Spark MLlib evaluation",
        "Model: Random Forest (numTrees=%d, maxDepth=%d, maxBins=%d, subsamplingRate=%.2f, "
        "minInstancesPerNode=%d, class-weighted, seed=%d)" % (
            RF_NUM_TREES, RF_MAX_DEPTH, RF_MAX_BINS, RF_SUBSAMPLING_RATE, RF_MIN_INSTANCES, SEED),
        "Input: " + INPUT_PATH,
        "Total valid records: %d" % total,
        "Legitimate records: %d" % legit_total,
        "Fraud records: %d" % fraud_total,
        "Fraud percentage: %.6f%%" % fraud_rate,
        "Training records: %d" % train_count,
        "Testing records: %d" % test_count,
        "",
        "Confusion matrix (test set, default decision rule):",
        "True negatives: %d" % tn,
        "False positives: %d" % fp,
        "False negatives: %d" % fn,
        "True positives: %d" % tp,
        "",
        "Accuracy: %.6f" % accuracy,
        "Fraud precision: %.6f" % precision,
        "Fraud recall: %.6f" % recall,
        "Fraud F1-score: %.6f" % f1,
        "ROC-AUC: %.6f" % auc_roc,
        "PR-AUC: %.6f" % auc_pr,
        "",
        "Precision/recall by probability threshold:",
    ] + threshold_lines + [
        "",
        "Feature importances (highest first):",
    ] + importance_lines + [
        "",
        "Saved model: " + MODEL_PATH,
        "Saved predictions: " + PREDICTIONS_PATH,
    ]

    os.makedirs(os.path.dirname(LOCAL_RESULTS), exist_ok=True)
    with open(LOCAL_RESULTS, "w") as result_file:
        result_file.write("\n".join(metrics) + "\n")

    print("\n=== MODEL EVALUATION ===")
    for line in metrics:
        print(line)
    print("\nSUCCESS: Part 3 completed.")
    print("Metrics saved locally:", LOCAL_RESULTS)

finally:
    for frame in (predictions, data):
        try:
            if frame is not None:
                frame.unpersist()
        except Exception:
            pass
    spark.stop()
