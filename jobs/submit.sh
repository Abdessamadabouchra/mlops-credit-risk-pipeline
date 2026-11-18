#!/bin/bash
# usage: submit.sh <script.py> [total_cores] [executor_mem] [driver_mem]
# Submits a job to the single-worker Spark cluster. Core/memory caps are explicit so that the two
# streaming jobs (1 core each) and one batch job (2 cores) always fit on the one worker.
set -euo pipefail
SCRIPT="$1"
CORES="${2:-2}"
EXEC_MEM="${3:-1g}"
DRIVER_MEM="${4:-768m}"

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --name "${SCRIPT%.py}" \
  --total-executor-cores "$CORES" \
  --executor-memory "$EXEC_MEM" \
  --driver-memory "$DRIVER_MEM" \
  --conf spark.driver.host="$(hostname -i | awk '{print $1}')" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.shuffle.partitions=8 \
  "/opt/jobs/$SCRIPT"
