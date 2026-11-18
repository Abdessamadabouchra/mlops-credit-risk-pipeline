"""Credit-risk scoring API.

Two modes:
- Docker Compose (default): serves the MLflow registered model aliased `champion` (fetched from MLflow / MinIO);
  POST /reload picks up a newly promoted champion.
- Kubernetes: MODEL_PATH points to a model baked into the image (exported from MLflow by k8s/deploy.ps1) and
  MODEL_VERSION gives its registry version. No MLflow/MinIO needed in the cluster; the model is loaded at startup
  and GET /ready (readiness probe) answers 200 only once it is loaded.
"""
import os
import threading

import mlflow
import mlflow.spark
from fastapi import FastAPI, HTTPException
from mlflow import MlflowClient
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession

from features import INPUT_SCHEMA, add_features

MODEL_NAME = "credit_risk"
MODEL_URI = f"models:/{MODEL_NAME}@champion"
MODEL_PATH = os.getenv("MODEL_PATH", "")          # set in the Kubernetes image
BAKED_VERSION = os.getenv("MODEL_VERSION", "")

app = FastAPI(title="Credit risk API", version="1.1")
_lock = threading.Lock()
_state = {"spark": None, "model": None, "version": None, "error": None}


class Application(BaseModel):
    age: int = Field(ge=18, le=100)
    income: float = Field(gt=0)
    employment_years: int = Field(ge=0)
    loan_amount: float = Field(gt=0)
    loan_term_months: int = Field(gt=0)
    interest_rate: float = Field(ge=0)
    credit_score: int = Field(ge=300, le=850)
    num_open_accounts: int = Field(ge=0)
    num_late_payments: int = Field(ge=0)
    debt_to_income: float = Field(ge=0)
    home_ownership: str
    loan_purpose: str


def _spark():
    if _state["spark"] is None:
        _state["spark"] = (SparkSession.builder.master("local[1]").appName("credit-risk-api")
                           .config("spark.ui.enabled", "false")
                           .config("spark.driver.memory", "1g").getOrCreate())
    return _state["spark"]


def _load_model():
    """Return the model to serve, loading it on first use (or when a new champion was promoted)."""
    if MODEL_PATH:                                   # Kubernetes: model baked into the image
        if _state["model"] is None:
            _spark()
            _state["model"] = mlflow.spark.load_model(MODEL_PATH, dfs_tmpdir="/tmp/mlflow_dfs")
            _state["version"] = BAKED_VERSION or "baked"
        return _state["model"]

    try:                                             # Docker Compose: champion from the MLflow registry
        version = MlflowClient().get_model_version_by_alias(MODEL_NAME, "champion").version
    except Exception:
        raise HTTPException(503, f"No '{MODEL_NAME}' champion model yet. Run the Airflow DAG credit_risk_pipeline first.")
    if _state["version"] != version:
        _spark()
        _state["model"] = mlflow.spark.load_model(MODEL_URI, dfs_tmpdir="/tmp/mlflow_dfs")
        _state["version"] = version
    return _state["model"]


@app.on_event("startup")
def _warm_up():
    """In Kubernetes, load the baked model in the background so the readiness probe can report it."""
    if not MODEL_PATH:
        return

    def load():
        try:
            with _lock:
                _load_model()
        except Exception as e:                       # surfaced by /ready
            _state["error"] = str(e)

    threading.Thread(target=load, daemon=True).start()


@app.get("/health")
def health():
    """Liveness: the process answers."""
    return {"status": "ok", "model_version": _state["version"]}


@app.get("/ready")
def ready():
    """Readiness: 200 only when a model is loaded and can serve predictions."""
    if _state["model"] is None:
        raise HTTPException(503, _state["error"] or "model loading")
    return {"status": "ready", "model_version": _state["version"]}


@app.post("/reload")
def reload_model():
    with _lock:
        _load_model()
    return {"model_version": _state["version"]}


@app.post("/predict")
def predict(app_in: Application):
    with _lock:
        model = _load_model()
        df = add_features(_spark().createDataFrame([tuple(getattr(app_in, f.name) for f in INPUT_SCHEMA.fields)],
                                                   schema=INPUT_SCHEMA))
        row = model.transform(df).select("probability", "prediction").first()
    p = float(row["probability"][1])
    return {"default_probability": round(p, 4),
            "prediction": int(row["prediction"]),
            "risk_level": "high" if p >= 0.5 else "medium" if p >= 0.2 else "low",
            "model_version": _state["version"]}
