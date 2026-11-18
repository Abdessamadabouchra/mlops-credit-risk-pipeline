"""Kafka -> bronze (raw, append-only Delta, partitioned by processing time)."""
import json
import os
import time

from kafka import KafkaConsumer, TopicPartition
from pyspark.sql import functions as F

from common import BRONZE, CHECKPOINTS, get_spark

TOPIC = os.environ.get("KAFKA_TOPIC", "credit_applications")
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
CHECKPOINT = f"{CHECKPOINTS}/bronze/credit_applications"

spark = get_spark("kafka_to_bronze")


def reset_checkpoint_if_topic_was_recreated():
    """If the topic was recreated (Kafka data lost), its offsets restart at 0 while the checkpoint remembers the old
    ones: the stream would then wait forever for offsets that never come. Detect it and move the stale checkpoint
    aside (kept, not deleted) so the stream restarts from the beginning of the new topic. Silver deduplicates on
    application_id, so re-reading is safe."""
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    offsets_dir = jvm.org.apache.hadoop.fs.Path(f"{CHECKPOINT}/offsets")
    fs = offsets_dir.getFileSystem(conf)
    if not fs.exists(offsets_dir):
        return
    batches = [int(s.getPath().getName()) for s in fs.listStatus(offsets_dir) if s.getPath().getName().isdigit()]
    if not batches:
        return
    last = spark.read.text(f"{CHECKPOINT}/offsets/{max(batches)}").collect()[-1].value
    committed = {int(p): o for p, o in json.loads(last).get(TOPIC, {}).items()}

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    parts = [TopicPartition(TOPIC, p) for p in (consumer.partitions_for_topic(TOPIC) or [])]
    end = {tp.partition: o for tp, o in consumer.end_offsets(parts).items()} if parts else {}
    consumer.close()

    ahead = {p: (o, end.get(p, 0)) for p, o in committed.items() if o > end.get(p, 0)}
    if ahead:
        stale = jvm.org.apache.hadoop.fs.Path(f"{CHECKPOINT}_stale_{int(time.time())}")
        fs.rename(jvm.org.apache.hadoop.fs.Path(CHECKPOINT), stale)
        print(f"[WARN] topic {TOPIC} was recreated (checkpoint offsets ahead of topic: {ahead}); "
              f"stale checkpoint moved to {stale}, restarting from the earliest offsets", flush=True)


reset_checkpoint_if_topic_was_recreated()

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .option("maxOffsetsPerTrigger", 20000)
       .load())

bronze = (raw.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("raw_json"),
            "topic", "partition", "offset",
            F.col("timestamp").alias("kafka_ts"))
          .withColumn("ingest_ts", F.current_timestamp())
          .withColumn("ingest_date", F.to_date("ingest_ts"))
          .withColumn("ingest_hour", F.hour("ingest_ts")))

query = (bronze.writeStream.format("delta")
         .outputMode("append")
         .partitionBy("ingest_date", "ingest_hour")
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="30 seconds")
         .start(BRONZE))
query.awaitTermination()
