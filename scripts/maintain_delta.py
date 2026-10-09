"""Delta table maintenance: VACUUM away files no table version needs any more.

    docker compose run --rm spark python scripts/maintain_delta.py                  # keep 7 days
    docker compose run --rm spark python scripts/maintain_delta.py --retain-hours 0 # reclaim all

Why files pile up: Delta never edits a Parquet file in place. A MERGE or overwrite writes
NEW files and records in _delta_log which ones are current; the old ones stay for time
travel. A job that crashes mid-write (e.g. disk full) leaves orphan files that were never
committed - invisible to readers, but still using disk. VACUUM deletes both kinds.

--retain-hours 0 also deletes the files older versions need, i.e. it ends time travel
for those versions. Delta blocks values under 168 h unless you disable the safety check,
because a long-running reader could still be using them. Fine on a laptop, think twice in
production.
"""

from __future__ import annotations

import argparse
import os

from lakehouse.config import load_settings
from lakehouse.spark_session import get_spark

TABLES = [
    "silver/klines",
    "silver/klines_quarantine",
    "gold/fact_prices",
    "gold/dim_coin",
    "gold/dim_date",
    "gold/dim_exchange",
]


def dir_mb(path: str) -> float:
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total / 1e6


def main() -> int:
    from delta.tables import DeltaTable

    ap = argparse.ArgumentParser()
    ap.add_argument("--retain-hours", type=float, default=168.0)
    args = ap.parse_args()

    settings = load_settings()
    spark = get_spark("delta-maintenance")
    if args.retain_hours < 168:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    print(f"{'table':<26}{'before MB':>11}{'after MB':>10}")
    freed = 0.0
    for t in TABLES:
        layer, name = t.split("/")
        path = f"{settings.layer_path(layer, for_spark=True)}/{name}"
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        before = dir_mb(path)
        DeltaTable.forPath(spark, path).vacuum(args.retain_hours)
        after = dir_mb(path)
        freed += before - after
        print(f"{t:<26}{before:>11.1f}{after:>10.1f}")
    print(f"\nfreed {freed:.1f} MB")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
