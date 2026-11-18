"""Export the current gold Delta table to a local Parquet snapshot (data/gold_snapshot) so that DVC can version it.
Gold is small, so the driver collects it with pandas; the Delta version is written next to it for traceability."""
import json
import os

from delta.tables import DeltaTable

from common import GOLD, get_spark

OUT = os.environ.get("SNAPSHOT_DIR", "/project/data/gold_snapshot")

spark = get_spark("export_gold")
version = DeltaTable.forPath(spark, GOLD).history(1).select("version").first()[0]
pdf = spark.read.format("delta").load(GOLD).toPandas()

os.makedirs(OUT, exist_ok=True)
pdf.to_parquet(os.path.join(OUT, "gold.parquet"), index=False)
with open(os.path.join(OUT, "meta.json"), "w") as f:
    json.dump({"delta_version": int(version), "rows": len(pdf), "positive_rate": float(pdf["label"].mean())}, f)
print(f"exported {len(pdf)} rows (gold delta version {version}) to {OUT}")
