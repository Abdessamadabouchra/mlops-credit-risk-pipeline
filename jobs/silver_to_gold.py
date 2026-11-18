"""silver -> gold: the ML contract (one row per application, engineered features + label)."""
from pyspark.sql import functions as F

from common import GOLD, SILVER, get_spark
from features import CATEGORICAL, NUMERIC, add_features

spark = get_spark("silver_to_gold")

silver = spark.read.format("delta").load(SILVER)
gold = (add_features(silver)
        .select("application_id", "event_date", *NUMERIC, *CATEGORICAL,
                F.col("default").cast("int").alias("label"))
        .withColumn("snapshot_date", F.current_date())
        .coalesce(2))

n = gold.count()
if n == 0:
    raise SystemExit("silver is empty - nothing to build")

(gold.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .save(GOLD))
print(f"gold written: {n} rows, positive rate = "
      f"{spark.read.format('delta').load(GOLD).agg(F.avg('label')).first()[0]:.3f}")
