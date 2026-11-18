"""Batch part of the pipeline (the kafka->bronze streaming job is a long-running Compose service).

Every task runs in a one-off container of the mlops-spark image, started through the Docker socket;
Spark tasks are submitted to the single-worker Spark cluster.

    bronze_to_silver -> compact_lake -> silver_to_gold -> check_new_data -> dvc_snapshot -> train_models -> promote_model

check_new_data is the conditional-retraining gate: when gold has grown by less than MIN_NEW_DATA_RATIO since the
last model, it exits with code 99, Airflow marks it SKIPPED and the downstream tasks are skipped too
(no useless DVC version, no useless model version).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

IMAGE = "mlops-spark:1.0"
ENV_KEYS = ["MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY", "MLFLOW_S3_ENDPOINT_URL", "MLFLOW_TRACKING_URI",
            "KAFKA_BOOTSTRAP", "KAFKA_TOPIC", "SPARK_MASTER_URL", "PROMOTION_TOLERANCE", "MIN_NEW_DATA_RATIO"]
ENV = {k: os.environ[k] for k in ENV_KEYS if k in os.environ}

# Host folder of the project (git + DVC repository), set by start.ps1. Needed by the dvc_snapshot task only.
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")
SKIP_EXIT_CODE = 99


def job(task_id: str, command: str, **kwargs) -> DockerOperator:
    extra_env = kwargs.pop("extra_env", {})
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        command=command,
        # unique per run and per retry: a leftover container (e.g. Airflow killed mid-task) can never block the next run
        container_name=f"mlops_job_{task_id}_{{{{ ts_nodash }}}}_{{{{ ti.try_number }}}}",
        docker_url="unix://var/run/docker.sock",
        network_mode="mlops-net",
        environment={**ENV, **extra_env},
        auto_remove="force",
        mount_tmp_dir=False,
        force_pull=False,        # the image is built/pulled locally; never pull it
        **kwargs,
    )


with DAG(
    dag_id="credit_risk_pipeline",
    description="silver -> compact -> gold -> [new data?] -> DVC snapshot -> train (SparkML + MLflow) -> promote",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["mlops", "credit-risk"],
) as dag:
    # batch jobs get 2 cores; 1 core of the single worker stays with the kafka->bronze streaming job
    silver = job("bronze_to_silver", "/opt/jobs/submit.sh bronze_to_silver.py 2 1g")
    compact = job("compact_lake", "/opt/jobs/submit.sh compact.py 2 1g")
    gold = job("silver_to_gold", "/opt/jobs/submit.sh silver_to_gold.py 2 1g")

    check = job("check_new_data", "/opt/jobs/submit.sh check_new_data.py 1 512m",
                skip_on_exit_code=SKIP_EXIT_CODE, retries=0)

    # mounts the project folder: exports gold to data/gold_snapshot, dvc add + push to MinIO, git commit.
    # Its last log line (the DVC hash) is pushed to XCom and handed to the training task.
    # NOTE the trailing space: Airflow treats any command ENDING in ".sh" as a Jinja template file to load
    # ("... not found in search path"). The space is ignored by Docker.
    dvc = job("dvc_snapshot", "bash /opt/jobs/dvc_snapshot.sh ",
              user="0:0",
              mounts=[Mount(target="/project", source=HOST_PROJECT_DIR, type="bind")] if HOST_PROJECT_DIR else [],
              do_xcom_push=True)

    train = job("train_models", "/opt/jobs/submit.sh train.py 2 1g 1g",
                extra_env={"DVC_DATA_HASH": "{{ ti.xcom_pull(task_ids='dvc_snapshot') }}"})
    promote = job("promote_model", "python3 /opt/jobs/promote.py")

    silver >> compact >> gold >> check >> dvc >> train >> promote
