"""bronze -> silver: parse, type, validate, deduplicate (MERGE), quarantine bad rows.

Runs as an incremental BATCH job (Airflow task): Structured Streaming with trigger(availableNow=True) processes
only the bronze data that arrived since the last run (tracked by the checkpoint), then stops. This frees the
core and memory a permanent streaming job would hold on the single worker, with exactly the same logic."""
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import BRONZE, CHECKPOINTS, SILVER, SILVER_REJECTED, get_spark
from features import RAW_SCHEMA

spark = get_spark("bronze_to_silver")

# The bronze table appears only after the first micro-batch of the bronze streaming job.
for _ in range(60):
    if DeltaTable.isDeltaTable(spark, BRONZE):
        break
    print("waiting for bronze table...", flush=True)
    time.sleep(10)
else:
    raise SystemExit("bronze table does not exist yet - is the stream-bronze service running?")


def process_batch(batch_df, batch_id):
    parsed = (batch_df
              .withColumn("j", F.from_json("raw_json", RAW_SCHEMA))
              .select("raw_json", "j.*")
              .withColumn("event_ts", F.to_timestamp("event_ts")))

    valid = F.coalesce(
        F.col("application_id").isNotNull() & F.col("event_ts").isNotNull()
        & (F.col("income") > 0) & F.col("age").between(18, 100)
        & (F.col("loan_amount") > 0) & (F.col("loan_term_months") > 0)
        & F.col("credit_score").between(300, 850) & F.col("default").isin(0, 1)
        & F.col("home_ownership").isNotNull() & F.col("loan_purpose").isNotNull(),
        F.lit(False))

    good = (parsed.filter(valid).drop("raw_json")
            .withColumn("event_date", F.to_date("event_ts"))
            .dropDuplicates(["application_id"]))
    bad = parsed.filter(~valid)

    if not bad.rdd.isEmpty():
        bad.write.format("delta").mode("append").save(SILVER_REJECTED)

    if not DeltaTable.isDeltaTable(spark, SILVER):
        good.write.format("delta").partitionBy("event_date").save(SILVER)
        return
    (DeltaTable.forPath(spark, SILVER).alias("t")
     .merge(good.alias("s"), "t.application_id = s.application_id")
     .whenNotMatchedInsertAll()
     .execute())


query = (spark.readStream.format("delta").load(BRONZE)
         .writeStream.foreachBatch(process_batch)
         .option("checkpointLocation", f"{CHECKPOINTS}/silver/credit_applications")
         .trigger(availableNow=True)
         .start())
query.awaitTermination()
silver_rows = spark.read.format("delta").load(SILVER).count() if DeltaTable.isDeltaTable(spark, SILVER) else 0
print(f"silver up to date: {silver_rows} rows", flush=True)
