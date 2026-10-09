"""Phase 0 smoke test for the Docker image - proves Spark + Delta work end to end:

    docker compose run --rm spark python scripts/check_spark.py

It writes a tiny Delta table, MERGEs (upserts) into it, and reads an older version back
with time travel - the three Delta features the project leans on.
"""

from lakehouse.config import load_settings
from lakehouse.spark_session import get_spark

settings = load_settings()
path = f"{settings.data_dir.as_posix()}/_smoke/prices_delta"
spark = get_spark("phase0-smoke")
print(f"Spark {spark.version} | Java {spark._jvm.java.lang.System.getProperty('java.version')}")

v0 = spark.createDataFrame(
    [("BTCUSDT", "2025-01-01", 93_400.0), ("ETHUSDT", "2025-01-01", 3_350.0)],
    "symbol STRING, trade_date STRING, close DOUBLE",
)
v0.write.format("delta").mode("overwrite").save(path)

from delta.tables import DeltaTable  # noqa: E402  (needs the session's JVM first)

updates = spark.createDataFrame(
    [("BTCUSDT", "2025-01-01", 94_000.0), ("SOLUSDT", "2025-01-01", 190.0)],
    "symbol STRING, trade_date STRING, close DOUBLE",
)
(
    DeltaTable.forPath(spark, path)
    .alias("t")
    .merge(updates.alias("u"), "t.symbol = u.symbol AND t.trade_date = u.trade_date")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

latest = spark.read.format("delta").load(path)
original = spark.read.format("delta").option("versionAsOf", 0).load(path)
assert latest.count() == 3 and original.count() == 2
btc = latest.filter("symbol = 'BTCUSDT'").first().close
assert btc == 94_000.0, "MERGE should have updated BTC"
print("Delta write + MERGE + time travel: OK")
latest.orderBy("symbol").show()
spark.stop()
