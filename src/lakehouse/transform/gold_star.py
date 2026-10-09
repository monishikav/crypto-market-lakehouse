"""Phase 3 - Gold: a star schema for analytics.

    docker compose run --rm spark python -m lakehouse.transform.gold_star
    docker compose run --rm spark python -m lakehouse.transform.gold_star \
        --start 2025-12 --end 2025-12

                     dim_date (1 row / day)
                            |
    dim_coin  ----  fact_prices (1 row / coin / minute)  ----  dim_exchange
    (1 row / coin)          grain: coin x minute x exchange      (1 row / venue)

Why a star schema? Facts (numbers you aggregate: price, volume) live in one tall table;
descriptive attributes (coin name, rank, weekday, quarter) live in small dimension tables.
Analysts filter/group by dimension attributes and aggregate fact measures - every BI tool
(and Athena's optimiser) is built around exactly this shape.

Surrogate keys are xxhash64(natural key) rather than an auto-increment sequence: the same
coin always gets the same key in every run, with no lookup table or join needed to assign
keys to 10M fact rows. (Trade-off: a 64-bit hash could in theory collide; at 20 coins the
probability is ~1e-17, and we assert uniqueness anyway.)
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import Settings, load_settings
from lakehouse.storage import get_storage

EXCHANGE = {"exchange_id": "binance", "exchange_name": "Binance", "exchange_type": "CEX",
            "website": "https://www.binance.com", "data_source": "data.binance.vision"}  # fmt: skip

# Explicit schema: CoinGecko returns 82940 for one coin and 0.2741 for another - schema
# inference would guess differently day to day. Contracts beat guesses.
COINGECKO_SCHEMA = T.StructType(
    [T.StructField(c, T.StringType()) for c in ("id", "symbol", "name", "image", "ath_date")]
    + [T.StructField("market_cap_rank", T.LongType())]
    + [
        T.StructField(c, T.DoubleType())
        for c in ("market_cap", "circulating_supply", "total_supply", "max_supply", "ath")
    ]
)


def key(*cols) -> F.Column:
    return F.xxhash64(*cols)


# --- dimensions ------------------------------------------------------------------------
def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}')) AS full_date")
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.weekday("full_date").alias("day_of_week"),  # 0 = Monday
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.weekofyear("full_date").alias("week_of_year"),
        (F.weekday("full_date") >= 5).alias("is_weekend"),
    )


def build_dim_coin(spark: SparkSession, settings: Settings, cg: DataFrame, bx: DataFrame):
    assets = spark.createDataFrame(
        [(a.symbol, a.coingecko_id) for a in settings.assets], "symbol STRING, coingecko_id STRING"
    )
    return (
        assets.join(bx, "symbol", "left")
        .join(cg.withColumnRenamed("id", "coingecko_id"), "coingecko_id", "left")
        .select(
            key("symbol").alias("coin_key"),
            "symbol",
            "base_asset",
            "quote_asset",
            "coingecko_id",
            F.col("name").alias("coin_name"),
            "market_cap_rank",
            F.col("market_cap").alias("market_cap_usd"),
            "circulating_supply",
            "max_supply",
            (F.col("max_supply").isNotNull()).alias("has_supply_cap"),
            F.col("ath").alias("all_time_high_usd"),
            F.to_timestamp("ath_date").alias("all_time_high_at"),
            F.col("status").alias("trading_status"),
            F.col("image").alias("logo_url"),
            "snapshot_date",
        )
    )


def build_dim_exchange(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([EXCHANGE]).select(
        key("exchange_id").alias("exchange_key"), *EXCHANGE.keys()
    )


# --- fact ------------------------------------------------------------------------------
def build_fact_prices(silver: DataFrame) -> DataFrame:
    """No joins needed: hash keys are computed from columns already on the row."""
    return silver.select(
        key("symbol").alias("coin_key"),
        F.date_format("trade_date", "yyyyMMdd").cast("int").alias("date_key"),
        key(F.lit(EXCHANGE["exchange_id"])).alias("exchange_key"),
        "open_time",
        "open",
        "high",
        "low",
        "close",
        F.col("volume").alias("base_volume"),
        F.col("quote_asset_volume").alias("quote_volume_usdt"),
        "number_of_trades",
        F.col("taker_buy_base_volume").alias("taker_buy_base_volume"),
        "trade_month",
    )


# --- metadata readers ------------------------------------------------------------------
def latest_snapshot(df: DataFrame) -> DataFrame:
    latest = df.agg(F.max("snapshot_date")).first()[0]
    return df.filter(F.col("snapshot_date") == latest)


def read_coingecko(spark: SparkSession, bronze: str) -> DataFrame:
    df = (
        spark.read.schema(COINGECKO_SCHEMA)
        .option("multiLine", True)
        .json(f"{bronze}/coingecko/coins_markets")
    )
    df = df.withColumn("snapshot_date", F.col("snapshot_date").cast("date"))
    # CoinGecko's "symbol" is "btc"; ours is "BTCUSDT". Rename to avoid an ambiguous join.
    return latest_snapshot(df).withColumnRenamed("symbol", "coingecko_symbol")


def read_exchange_info(spark: SparkSession, bronze: str) -> DataFrame:
    df = spark.read.option("multiLine", True).json(f"{bronze}/binance/exchange_info")
    df = latest_snapshot(df).select(F.explode("symbols").alias("s"))
    return df.select(
        F.col("s.symbol").alias("symbol"),
        F.col("s.baseAsset").alias("base_asset"),
        F.col("s.quoteAsset").alias("quote_asset"),
        F.col("s.status").alias("status"),
    )


# --- checks ----------------------------------------------------------------------------
def integrity_checks(fact: DataFrame, dims: dict[str, DataFrame]) -> dict[str, int]:
    """Every fact row must point at an existing dimension row (no orphans)."""
    out = {}
    for fk, dim in (("coin_key", "dim_coin"), ("date_key", "dim_date"),
                    ("exchange_key", "dim_exchange")):  # fmt: skip
        out[f"orphan_{fk}"] = fact.join(dims[dim], fk, "left_anti").count()
    dup = dims["dim_coin"].groupBy("coin_key").count().filter("count > 1").count()
    out["duplicate_coin_keys"] = dup
    return out


def main(argv: list[str] | None = None) -> int:
    from delta.tables import DeltaTable

    from lakehouse.spark_session import get_spark

    ap = argparse.ArgumentParser(description="Build the gold star schema")
    ap.add_argument("--start", help="YYYY-MM: rebuild only these fact months (replaceWhere)")
    ap.add_argument("--end", help="YYYY-MM")
    args = ap.parse_args(argv)

    started = time.perf_counter()
    settings = load_settings()
    spark = get_spark("gold-star")
    bronze = settings.layer_path("bronze", True)
    silver_path = f"{settings.layer_path('silver', True)}/klines"
    gold = settings.layer_path("gold", True)

    silver = spark.read.format("delta").load(silver_path)
    span = silver.agg(F.min("trade_date"), F.max("trade_date")).first()

    dims = {
        "dim_date": build_dim_date(spark, str(span[0]), str(span[1])),
        "dim_coin": build_dim_coin(
            spark, settings, read_coingecko(spark, bronze), read_exchange_info(spark, bronze)
        ),
        "dim_exchange": build_dim_exchange(spark),
    }
    for name, df in dims.items():  # dimensions are tiny: full refresh every run
        df.coalesce(1).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(f"{gold}/{name}")

    fact = build_fact_prices(silver)
    if args.start:
        fact = fact.filter(F.col("trade_month").between(args.start, args.end or args.start))
    # One file per month, sorted by coin then time: Parquet row-group min/max stats on
    # coin_key let Athena skip other coins' data inside the file.
    fact = fact.repartition("trade_month").sortWithinPartitions("coin_key", "open_time")
    writer = fact.write.format("delta").partitionBy("trade_month")
    fact_path = f"{gold}/fact_prices"
    if args.start and DeltaTable.isDeltaTable(spark, fact_path):
        # replaceWhere = atomically swap ONLY these months. Idempotent, and cheaper than
        # MERGE when you are recomputing whole partitions anyway.
        cond = f"trade_month BETWEEN '{args.start}' AND '{args.end or args.start}'"
        writer.mode("overwrite").option("replaceWhere", cond).save(fact_path)
    else:
        writer.mode("overwrite").option("overwriteSchema", "true").save(fact_path)

    gold_dims = {n: spark.read.format("delta").load(f"{gold}/{n}") for n in dims}
    fact_all = spark.read.format("delta").load(fact_path)
    checks = integrity_checks(fact_all, gold_dims)
    detail = DeltaTable.forPath(spark, fact_path).detail().first()
    summary = {
        "run_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "rows": {n: df.count() for n, df in gold_dims.items()} | {"fact_prices": fact_all.count()},
        "fact_files": detail["numFiles"],
        "fact_size_mb": round(detail["sizeInBytes"] / 1e6, 1),
        "integrity": checks,
        "seconds": round(time.perf_counter() - started, 1),
    }
    get_storage(settings, "gold").write_bytes(
        "_manifests/star_run.json", json.dumps(summary, indent=2).encode()
    )
    print(json.dumps(summary, indent=2))
    spark.stop()
    if any(checks.values()):
        print("INTEGRITY CHECK FAILED")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
