"""Create the MinIO buckets used by the project (idempotent, retries until MinIO answers)."""
import os
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

BUCKETS = ["lake", "mlflow", "dvc", "checkpoints"]

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

for attempt in range(30):
    try:
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        for b in BUCKETS:
            if b in existing:
                print(f"bucket '{b}' already exists")
            else:
                s3.create_bucket(Bucket=b)
                print(f"bucket '{b}' created")
        break
    except (EndpointConnectionError, ClientError) as e:
        print(f"MinIO not ready ({e}), retry {attempt + 1}/30")
        time.sleep(2)
else:
    raise SystemExit("could not create buckets")
