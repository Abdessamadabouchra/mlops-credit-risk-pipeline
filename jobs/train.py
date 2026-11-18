"""Train 4 models on gold (SparkML: logistic regression, random forest, GBT + XGBoost via xgboost.spark),
track everything in MLflow, register the best one."""
import os
import uuid

import mlflow
import mlflow.spark
from delta.tables import DeltaTable
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from xgboost.spark import SparkXGBClassifier

from common import GOLD, get_spark
from features import CATEGORICAL, NUMERIC

MODEL_NAME = "credit_risk"


def _patch_mlflow_for_s3a():
    """mlflow.spark copies the model from the DFS tmp dir with the *default* Hadoop FS (file:///), which
    rejects s3a:// paths ('Wrong FS'). Use the filesystem that belongs to the source path instead."""
    import os
    from mlflow.spark import _HadoopFileSystem as H

    def copy_to_local_file(cls, src, dst, remove_src):
        src_path = cls._remote_path(src)
        fs = src_path.getFileSystem(cls._conf())
        dst_path = cls._jvm().org.apache.hadoop.fs.Path("file://" + os.path.abspath(dst))
        fs.copyToLocalFile(remove_src, src_path, dst_path, True)

    H.copy_to_local_file = classmethod(copy_to_local_file)


_patch_mlflow_for_s3a()
SPLIT_RULE = "pmod(hash(application_id),100) < 80 -> train"

spark = get_spark("train")
mlflow.set_experiment("credit_risk")   # MLFLOW_TRACKING_URI comes from the environment

df = spark.read.format("delta").load(GOLD)
gold_version = DeltaTable.forPath(spark, GOLD).history(1).select("version").first()[0]

# deterministic split on the id: reruns on the same data give comparable metrics
df = df.withColumn("bucket", F.pmod(F.hash("application_id"), F.lit(100)))
train, test = df.filter("bucket < 80").cache(), df.filter("bucket >= 80").cache()
n_train, n_test = train.count(), test.count()
if n_train < 200 or n_test < 50:
    raise SystemExit(f"not enough data to train (train={n_train}, test={n_test}); let the generator run longer")


def preprocessing(scale: bool):
    stages = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]
    stages.append(OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL],
                                outputCols=[f"{c}_oh" for c in CATEGORICAL], handleInvalid="keep"))
    assembler_out = "features_raw" if scale else "features"
    stages.append(VectorAssembler(inputCols=NUMERIC + [f"{c}_oh" for c in CATEGORICAL],
                                  outputCol=assembler_out, handleInvalid="keep"))
    if scale:
        stages.append(StandardScaler(inputCol="features_raw", outputCol="features"))
    return stages


candidates = {
    "logistic_regression": (True, LogisticRegression(labelCol="label", maxIter=50, regParam=0.01)),
    "random_forest": (False, RandomForestClassifier(labelCol="label", numTrees=100, maxDepth=8, seed=42)),
    "gbt": (False, GBTClassifier(labelCol="label", maxIter=40, maxDepth=5, seed=42)),
    # official XGBoost <-> Spark integration; num_workers must fit in the job's cores (2 on the single worker)
    "xgboost": (False, SparkXGBClassifier(features_col="features", label_col="label", num_workers=2,
                                          n_estimators=200, max_depth=6, learning_rate=0.1,
                                          random_state=42)),
}
XGB_PARAMS = ("num_workers", "n_estimators", "max_depth", "learning_rate", "random_state")

auc_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
pr_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")

best = None
for name, (scale, clf) in candidates.items():
    with mlflow.start_run(run_name=name) as run:
        model = Pipeline(stages=preprocessing(scale) + [clf]).fit(train)
        pred = model.transform(test)
        auc, pr = auc_eval.evaluate(pred), pr_eval.evaluate(pred)

        mlflow.log_params({"algorithm": name, "split_rule": SPLIT_RULE, "n_train": n_train, "n_test": n_test,
                           **{k.name: v for k, v in clf.extractParamMap().items()
                              if k.name in ("maxIter", "regParam", "numTrees", "maxDepth", "seed") + XGB_PARAMS}})
        mlflow.log_metrics({"auc": auc, "auc_pr": pr})
        mlflow.set_tags({"gold_delta_version": str(gold_version),
                         "dvc_data_hash": os.getenv("DVC_DATA_HASH", "") or "not-versioned",
                         "features": ",".join(NUMERIC + CATEGORICAL)})
        # dfs_tmpdir on MinIO: executors on the worker write the model data there, not on their local disk
        mlflow.spark.log_model(model, "model", dfs_tmpdir=f"s3a://mlflow/dfs_tmp/{uuid.uuid4()}",
                               pip_requirements=["pyspark==3.5.1", "xgboost-cpu==2.1.1"])
        print(f"{name}: auc={auc:.4f} auc_pr={pr:.4f}", flush=True)
        if best is None or auc > best[1]:
            best = (run.info.run_id, auc, name)

run_id, auc, name = best
mv = mlflow.register_model(f"runs:/{run_id}/model", MODEL_NAME)
print(f"registered {MODEL_NAME} v{mv.version} ({name}, auc={auc:.4f})", flush=True)

# xgboost.spark trains in Spark *barrier* mode. In Spark 3.5.1 the BarrierCoordinator starts a NON-daemon timer
# thread ("barrier epoch increment timer") that is never cancelled, so the driver JVM never exits after the script
# ends and the Airflow task hangs in "running" (fixed in Spark 4). All work is done at this point (models logged
# and registered), so stop Spark and terminate the JVM explicitly.
jvm = spark.sparkContext._jvm
spark.stop()
try:
    jvm.System.exit(0)          # the py4j call never returns: the JVM (our parent process) exits
except Exception:
    pass
os._exit(0)
