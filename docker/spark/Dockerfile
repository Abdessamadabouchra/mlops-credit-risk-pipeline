FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

# Jars: S3A (MinIO), Kafka source, Delta Lake. Versions are matched to Spark 3.5.1 / Hadoop 3.3.4.
ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && for u in \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
      io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar ; \
    do curl -fsSLO "$MVN/$u" || exit 1; done

# Python libs (pyspark itself comes from /opt/spark/python via PYTHONPATH, same version as the cluster)
RUN pip3 install --no-cache-dir "numpy<2" "pandas==2.1.4" "mlflow==2.16.2" boto3 psycopg2-binary \
        "kafka-python==2.0.2" "fastapi==0.115.0" "uvicorn==0.30.6" "pydantic==2.9.2" importlib-metadata \
    && pip3 install --no-cache-dir --no-deps "delta-spark==3.2.0" \
    && pip3 install --no-cache-dir "dvc[s3]==3.55.2" "pathspec==0.11.2"   # dvc 3.55 breaks with pathspec >= 0.12

# separate layer so that adding these does not reinstall everything above
# xgboost-cpu = XGBoost without the CUDA/NCCL wheels (much smaller); streamlit = web UI
RUN pip3 install --no-cache-dir "xgboost-cpu==2.1.1" "streamlit==1.39.0"

ENV PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/jobs:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

COPY jobs /opt/jobs
COPY generator /opt/generator
COPY api /opt/api
COPY ui /opt/ui
RUN sed -i 's/\r$//' /opt/jobs/submit.sh /opt/jobs/dvc_snapshot.sh && chmod +x /opt/jobs/submit.sh /opt/jobs/dvc_snapshot.sh

USER spark
WORKDIR /opt/spark/work-dir
