"""Give the alias `champion` to the newest model version if it is at least as good as the current champion.

The two AUCs are measured on different test sets (gold grows between runs, and the test split is a hash of the
id), so a strict "new > old" rule would keep an old model trained on much less data because of noise.
The new version wins unless it is worse by more than PROMOTION_TOLERANCE (default 0.01 AUC)."""
import os

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

MODEL_NAME = "credit_risk"
TOLERANCE = float(os.getenv("PROMOTION_TOLERANCE", "0.01"))
client = MlflowClient()

versions = client.search_model_versions(f"name='{MODEL_NAME}'")
if not versions:
    raise SystemExit("no registered model version yet")
latest = max(versions, key=lambda v: int(v.version))
new_run = client.get_run(latest.run_id)
new_auc = new_run.data.metrics["auc"]
new_n = new_run.data.params.get("n_train", "?")

try:
    champ = client.get_model_version_by_alias(MODEL_NAME, "champion")
except MlflowException:
    champ = None

if champ is not None and champ.version == latest.version:
    print(f"v{latest.version} is already the champion")
    raise SystemExit(0)

if champ is None:
    promote, reason = True, "no champion yet"
else:
    champ_run = client.get_run(champ.run_id)
    champ_auc = champ_run.data.metrics["auc"]
    champ_n = champ_run.data.params.get("n_train", "?")
    promote = new_auc >= champ_auc - TOLERANCE
    reason = (f"new auc={new_auc:.4f} (n_train={new_n}) vs champion v{champ.version} auc={champ_auc:.4f} "
              f"(n_train={champ_n}), tolerance={TOLERANCE}")

client.set_model_version_tag(MODEL_NAME, latest.version, "promotion_decision",
                             ("promoted: " if promote else "rejected: ") + reason)
if promote:
    client.set_registered_model_alias(MODEL_NAME, "champion", latest.version)
    print(f"v{latest.version} promoted to champion ({reason})")
else:
    print(f"v{latest.version} NOT promoted ({reason})")
