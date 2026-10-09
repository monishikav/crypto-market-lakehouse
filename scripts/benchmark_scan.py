"""Measure what partitioning + Delta + Parquet actually buy us: bronze CSV vs silver Delta.

    docker compose run --rm spark python scripts/benchmark_scan.py

For each query we record the bytes Spark *really* read from disk (from Spark's own task
metrics, via the Spark UI REST API) and wall-clock time. These are your resume numbers.
"""

from __future__ import annotations

import json
import time
import urllib.request

from pyspark.sql import functions as F

from lakehouse.config import load_settings
from lakehouse.spark_session import get_spark
from lakehouse.storage import get_storage
from lakehouse.transform.silver_klines import read_bronze, standardize

settings = load_settings()
spark = get_spark("benchmark-scan")
sc = spark.sparkContext
bronze = standardize(read_bronze(spark, settings.layer_path("bronze", True), settings.interval))
silver = spark.read.format("delta").load(f"{settings.layer_path('silver', True)}/klines")

QUERIES = {
    "1 coin, 1 day": lambda df: df.filter(
        (F.col("symbol") == "BTCUSDT") & (F.col("trade_date") == "2025-06-15")
    ).agg(F.avg("close"), F.sum("volume")),
    "20 coins, 1 month": lambda df: (
        df.filter(F.col("trade_date").between("2025-06-01", "2025-06-30"))
        .groupBy("symbol")
        .agg(F.sum("quote_asset_volume"))
    ),
    "full year, 2 columns": lambda df: df.groupBy("symbol").agg(F.max("high"), F.count("*")),
}


def bytes_read(group: str) -> int:
    time.sleep(1.5)  # let Spark's listener bus publish the finished stages
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    total = 0
    for job_id in sc.statusTracker().getJobIdsForGroup(group):
        for stage_id in sc.statusTracker().getJobInfo(job_id).stageIds:
            with urllib.request.urlopen(f"{base}/stages/{stage_id}") as r:
                total += sum(attempt.get("inputBytes", 0) for attempt in json.load(r))
    return total


def measure(label: str, df) -> dict:
    sc.setJobGroup(label, label)
    t0 = time.perf_counter()
    df.collect()
    secs = time.perf_counter() - t0
    return {"bytes": bytes_read(label), "seconds": round(secs, 2)}


rows = []
for name, q in QUERIES.items():
    b = measure(f"bronze:{name}", q(bronze))
    s = measure(f"silver:{name}", q(silver))
    reduction = 100 * (1 - s["bytes"] / b["bytes"]) if b["bytes"] else 0
    rows.append(
        {"query": name, "bronze": b, "silver": s, "scan_reduction_pct": round(reduction, 1)}
    )

cols = ["bronze MB", "silver MB", "less data", "bronze s", "silver s"]
print(
    f"\n{'query':<22}"
    + "".join(f"{c:>11}" for c in cols[:3])
    + "".join(f"{c:>10}" for c in cols[3:])
)
for r in rows:
    print(
        f"{r['query']:<22}{r['bronze']['bytes'] / 1e6:>11.1f}{r['silver']['bytes'] / 1e6:>11.1f}"
        f"{r['scan_reduction_pct']:>10.1f}%{r['bronze']['seconds']:>10.2f}{r['silver']['seconds']:>10.2f}"
    )
get_storage(settings, "silver").write_bytes(
    "_manifests/benchmark_scan.json", json.dumps(rows, indent=2).encode()
)
spark.stop()
