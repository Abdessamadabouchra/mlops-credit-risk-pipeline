"""Housekeeping: compact the small files created by streaming (OPTIMIZE) and clean old versions (VACUUM)."""
from delta.tables import DeltaTable

from common import BRONZE, GOLD, SILVER, get_spark

spark = get_spark("compact")

for name, path in [("bronze", BRONZE), ("silver", SILVER), ("gold", GOLD)]:
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[{name}] no table yet, skipping")
        continue
    table = DeltaTable.forPath(spark, path)
    before = table.detail().select("numFiles").first()[0]
    table.optimize().executeCompaction()
    table.vacuum(168)   # keep 7 days of history (time travel / running readers stay safe)
    after = DeltaTable.forPath(spark, path).detail().select("numFiles").first()[0]
    print(f"[{name}] files: {before} -> {after}")
