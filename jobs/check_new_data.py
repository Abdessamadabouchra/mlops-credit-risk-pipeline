"""Conditional retraining gate.

Compares the number of rows in gold with the number of rows the latest registered model was trained and
tested on (n_train + n_test, logged in MLflow). Exits with code 99 when there is not enough new data:
Airflow maps that exit code to SKIPPED, so dvc_snapshot / train / promote are skipped for this run.
"""
import os
import sys

from mlflow import MlflowClient

from common import GOLD, get_spark

SKIP_EXIT_CODE = 99
MODEL_NAME = "credit_risk"
MIN_RATIO = float(os.getenv("MIN_NEW_DATA_RATIO", "0.05"))

spark = get_spark("check_new_data")
rows = spark.read.format("delta").load(GOLD).count()
spark.stop()

client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    print(f"gold has {rows} rows and no model is registered yet -> TRAIN", flush=True)
    sys.exit(0)

latest = max(versions, key=lambda v: int(v.version))
params = client.get_run(latest.run_id).data.params
trained_on = int(params["n_train"]) + int(params["n_test"])
ratio = (rows - trained_on) / trained_on

msg = (f"gold has {rows} rows, model v{latest.version} was built on {trained_on} rows "
       f"-> {ratio:+.1%} new data (threshold {MIN_RATIO:.0%})")
if ratio >= MIN_RATIO:
    print(msg + " -> TRAIN", flush=True)
    sys.exit(0)
print(msg + " -> SKIP retraining", flush=True)
sys.exit(SKIP_EXIT_CODE)
