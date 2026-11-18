"""Spark session factory (MinIO via S3A + Delta Lake) and lake paths."""
import os

from pyspark.sql import SparkSession

LAKE = "s3a://lake"
BRONZE = f"{LAKE}/bronze/credit_applications"
SILVER = f"{LAKE}/silver/credit_applications"
SILVER_REJECTED = f"{LAKE}/silver/_rejected/credit_applications"
GOLD = f"{LAKE}/gold/credit_risk_features"
CHECKPOINTS = "s3a://checkpoints"


def get_spark(app_name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(app_name)
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
             .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
