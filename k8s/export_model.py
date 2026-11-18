"""Export the MLflow champion model to /k8s/build/model so it can be baked into the serving image.
Run inside the mlops-spark image on the mlops-net network (done by k8s/deploy.ps1)."""
import json
import shutil

import mlflow
from mlflow import MlflowClient

MODEL_NAME = "credit_risk"
OUT = "/k8s/build"

client = MlflowClient()
mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
run = client.get_run(mv.run_id)

shutil.rmtree(f"{OUT}/model", ignore_errors=True)
mlflow.artifacts.download_artifacts(artifact_uri=f"models:/{MODEL_NAME}@champion", dst_path=f"{OUT}/model")

info = {"version": mv.version, "algorithm": run.data.params.get("algorithm"),
        "auc": run.data.metrics.get("auc"), "dvc_data_hash": run.data.tags.get("dvc_data_hash")}
with open(f"{OUT}/model_info.json", "w") as f:
    json.dump(info, f)
print(json.dumps(info))
