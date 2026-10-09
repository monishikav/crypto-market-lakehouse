"""Phase 2 - Silver: bronze CSV candles -> clean, typed, deduplicated Delta table.

Run inside Docker (Spark lives there):
    docker compose run --rm --service-ports spark python -m lakehouse.transform.silver_klines
    docker compose run --rm spark python -m lakehouse.transform.silver_klines \
        --mode incremental --start 2025-12 --end 2025-12

What silver adds on top of bronze:
  1. Schema    - explicit types (DECIMAL for money, never float), not strings.
  2. Time      - Binance switched spot files from milliseconds to MICROseconds in 2025.
                 We detect the unit per row and normalise to a proper UTC timestamp.
  3. Dedupe    - one row per (symbol, open_time) even if monthly + daily files overlap.
  4. Quality   - impossible candles (high < low, negative volume...) are not silently
                 dropped: they go to a quarantine table with the reason, and get counted.
  5. Layout    - Delta, partitioned by symbol + month (see PARTITIONING below).
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import load_settings
from lakehouse.ingestion.binance_bulk import KLINE_COLUMNS, build_periods
from lakehouse.storage import get_storage

# DECIMAL(28,8): 8 decimal places like Binance, room for 20 integer digits (quote volume).
# Floats can't represent 0.1 exactly - summing money as DOUBLE drifts. Interviewers ask this.
MONEY = T.DecimalType(28, 8)

# Read prices as STRING first, then cast: a malformed value becomes NULL (caught by the
# quality rules) instead of failing the whole job or being silently mangled by the parser.
RAW_SCHEMA = T.StructType(
    [
        T.StructField("open_time", T.LongType()),
        T.StructField("open", T.StringType()),
        T.StructField("high", T.StringType()),
        T.StructField("low", T.StringType()),
        T.StructField("close", T.StringType()),
        T.StructField("volume", T.StringType()),
        T.StructField("close_time", T.LongType()),
        T.StructField("quote_asset_volume", T.StringType()),
        T.StructField("number_of_trades", T.LongType()),
        T.StructField("taker_buy_base_volume", T.StringType()),
        T.StructField("taker_buy_quote_volume", T.StringType()),
        T.StructField("ignore", T.StringType()),
    ]
)
assert RAW_SCHEMA.fieldNames() == KLINE_COLUMNS

MONEY_COLS = [
    "open", "high", "low", "close", "volume",
    "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume",
]  # fmt: skip

# PARTITIONING: symbol + trade_month, NOT symbol + trade_date.
#   by day   -> 20 x 365 = 7,300 folders of ~1,440 rows (~60 KB files): the "small files
#               problem" - Spark/Athena spend more time opening files than reading them.
#   by month -> 20 x 12 = 240 folders of ~44,640 rows (~1-2 MB files).
# Day-level filters still skip data: Delta keeps min/max stats per file on trade_date.
PARTITION_COLS = ["symbol", "trade_month"]
KEY_COLS = ["symbol", "open_time"]


# --- read ------------------------------------------------------------------------------
def read_bronze(spark: SparkSession, bronze_root: str, interval: str, periods=None) -> DataFrame:
    base = f"{bronze_root}/binance/klines/interval={interval}"
    if periods:  # incremental: only the files for these months/days
        paths = [f"{base}/symbol=*/*-{interval}-{p}.csv.gz" for p in periods]
    else:
        paths = [base]
    return (
        spark.read.option("basePath", base)  # keeps symbol=... as a column
        .schema(RAW_SCHEMA)
        .csv(paths, header=False)
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )


# --- transform (pure: no I/O, so it's unit-testable) ----------------------------------
def to_utc_timestamp(epoch_col: str):
    """ms (13 digits) or us (16 digits) -> TIMESTAMP. 1e15 us = 2001; 1e15 ms = year 33658."""
    c = F.col(epoch_col)
    micros = F.when(c >= F.lit(10**15), c).otherwise(c * 1000)
    return F.timestamp_micros(micros)


def standardize(raw: DataFrame) -> DataFrame:
    df = raw.select(
        F.col("symbol"),
        to_utc_timestamp("open_time").alias("open_time"),
        to_utc_timestamp("close_time").alias("close_time"),
        # try_cast, not cast: Spark 4 runs in ANSI mode, where one malformed value would
        # crash the whole job. try_cast turns it into NULL -> caught by quality_flags().
        *[F.col(c).try_cast(MONEY).alias(c) for c in MONEY_COLS],
        F.col("number_of_trades"),
        F.when(F.col("open_time") >= F.lit(10**15), "us").otherwise("ms").alias("_ts_unit"),
        F.col("_source_file"),
    )
    # Dates derived in UTC (session timezone is pinned to UTC in spark_session.py).
    return df.withColumn("trade_date", F.to_date("open_time")).withColumn(
        "trade_month", F.date_format("open_time", "yyyy-MM")
    )


def quality_flags(df: DataFrame) -> DataFrame:
    """Adds `_dq_error`: NULL when the candle is valid, else the first rule it breaks."""
    rules = [
        (F.col("open_time").isNull(), "null_open_time"),
        (reduce(or_, [F.col(c).isNull() for c in ("open", "high", "low", "close")]), "null_price"),
        (F.least("open", "high", "low", "close") <= 0, "non_positive_price"),
        (F.col("high") < F.greatest("open", "close", "low"), "high_below_ohlc"),
        (F.col("low") > F.least("open", "close", "high"), "low_above_ohlc"),
        (F.col("volume") < 0, "negative_volume"),
        (F.col("close_time") <= F.col("open_time"), "close_before_open"),
    ]
    expr = F.lit(None).cast("string")
    for cond, name in reversed(rules):  # first matching rule wins
        expr = F.when(cond, F.lit(name)).otherwise(expr)
    return df.withColumn("_dq_error", expr)


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per (symbol, open_time); deterministic tie-break on source file name."""
    w = Window.partitionBy(*KEY_COLS).orderBy(F.col("_source_file").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_silver(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (clean, rejected). The whole silver contract in one function."""
    flagged = quality_flags(standardize(raw))
    rejected = flagged.filter(F.col("_dq_error").isNotNull())
    clean = deduplicate(flagged.filter(F.col("_dq_error").isNull()).drop("_dq_error"))
    run_ts = F.lit(datetime.now(UTC).replace(tzinfo=None)).cast("timestamp")
    return clean.withColumn("_processed_at", run_ts), rejected.withColumn("_processed_at", run_ts)


# --- write -----------------------------------------------------------------------------
def write_delta(spark: SparkSession, df: DataFrame, path: str, mode: str) -> str:
    from delta.tables import DeltaTable

    # One shuffle so each (symbol, month) partition is written by ONE task -> 1 file per
    # partition instead of (shuffle partitions x partitions) tiny files.
    df = df.repartition(*PARTITION_COLS)
    if mode == "full" or not DeltaTable.isDeltaTable(spark, path):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(*PARTITION_COLS)
            .save(path)
        )
        return "overwrite"

    # Incremental: MERGE = upsert. Re-running the same day updates instead of duplicating.
    # Listing the touched months in the ON clause lets Delta prune every other partition.
    months = sorted(r.trade_month for r in df.select("trade_month").distinct().collect())
    month_list = ", ".join(f"'{m}'" for m in months)
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(
            df.alias("s"),
            f"t.trade_month IN ({month_list}) AND t.symbol = s.symbol "
            "AND t.open_time = s.open_time",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return f"merge({month_list})"


def main(argv: list[str] | None = None) -> int:
    from lakehouse.spark_session import get_spark

    settings = load_settings()
    ap = argparse.ArgumentParser(description="Build silver.klines (Delta) from bronze")
    ap.add_argument("--mode", choices=["full", "incremental"], default="full")
    ap.add_argument("--start", help="YYYY-MM or YYYY-MM-DD (incremental mode)")
    ap.add_argument("--end", help="YYYY-MM or YYYY-MM-DD (incremental mode)")
    args = ap.parse_args(argv)
    if args.mode == "incremental" and not (args.start and args.end):
        ap.error("--mode incremental needs --start and --end")

    started = time.perf_counter()
    spark = get_spark("silver-klines")
    bronze = settings.layer_path("bronze", for_spark=True)
    silver = settings.layer_path("silver", for_spark=True)
    periods = build_periods(args.start, args.end) if args.mode == "incremental" else None

    raw = read_bronze(spark, bronze, settings.interval, periods).cache()
    clean, rejected = build_silver(raw)

    rows_in = raw.count()
    action = write_delta(spark, clean, f"{silver}/klines", args.mode)
    n_rejected = rejected.count()
    if n_rejected:
        rejected.write.format("delta").mode("append").save(f"{silver}/klines_quarantine")

    from delta.tables import DeltaTable

    detail = DeltaTable.forPath(spark, f"{silver}/klines").detail().first()
    table = spark.read.format("delta").load(f"{silver}/klines")
    rows_out = table.count()
    by_unit = {r["_ts_unit"]: r["count"] for r in table.groupBy("_ts_unit").count().collect()}
    span = table.agg(F.min("open_time").alias("lo"), F.max("open_time").alias("hi")).first()

    summary = {
        "run_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "mode": args.mode,
        "write": action,
        "bronze_rows_read": rows_in,
        "rejected_rows": n_rejected,
        # full mode: table == this run's output, so no second pass over 10M rows is needed
        "duplicates_removed": rows_in
        - n_rejected
        - (rows_out if args.mode == "full" else clean.count()),
        "silver_rows_total": rows_out,
        "timestamp_units_seen": by_unit,
        "time_span_utc": [str(span["lo"]), str(span["hi"])],
        "delta_files": detail["numFiles"],
        "delta_size_mb": round(detail["sizeInBytes"] / 1e6, 1),
        "partitions": PARTITION_COLS,
        "seconds": round(time.perf_counter() - started, 1),
    }
    stamp = summary["run_at_utc"].replace(":", "").replace("-", "")
    get_storage(settings, "silver").write_bytes(
        f"_manifests/klines/run_{stamp}.json", json.dumps(summary, indent=2).encode()
    )
    print(json.dumps(summary, indent=2))
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
