"""Phase 6 - Real-time lane, part 2: Kafka -> Spark Structured Streaming -> Delta.

    docker compose up -d kafka
    docker compose run --rm --service-ports spark python -m lakehouse.streaming.trades_to_delta
    ... --seconds 300      # stop after 5 minutes and print a throughput summary

Two queries share one Kafka source:
  1. trades_raw     every trade appended to bronze/stream_trades       (audit trail, replay)
  2. ohlcv_1m       1-minute candles per symbol, upserted (MERGE) into silver/stream_ohlcv_1m

Concepts:
  * Checkpoints: each query stores its Kafka offsets + state in data/checkpoints/<query>.
    Restart the job and it resumes exactly where it stopped - no gaps, no duplicates.
  * Exactly-once into Delta: the raw sink is an idempotent Delta append keyed by batch id;
    the candle sink is a MERGE on (symbol, window_start), so replaying a batch after a crash
    rewrites the same values instead of double-counting.
  * Event time + watermark: candles are bucketed by when the TRADE happened (trade_time),
    not when Spark saw it. withWatermark("event_time", "2 minutes") tells Spark how long to
    wait for late trades before a minute is final and its state can be dropped - otherwise
    state would grow forever.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.config import load_settings
from lakehouse.storage import get_storage

TOPIC = "binance.trades"
PRICE = T.DecimalType(28, 8)

TRADE_SCHEMA = T.StructType(
    [
        T.StructField("symbol", T.StringType()),
        T.StructField("trade_id", T.LongType()),
        T.StructField("price", T.StringType()),
        T.StructField("qty", T.StringType()),
        T.StructField("trade_time_ms", T.LongType()),
        T.StructField("event_time_ms", T.LongType()),
        T.StructField("is_buyer_maker", T.BooleanType()),
        T.StructField("ingested_at_ms", T.LongType()),
    ]
)


# --- pure transforms (unit-tested on static DataFrames) --------------------------------
def parse_trades(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows (binary key/value + metadata) -> typed trades."""
    t = F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t")
    return kafka_df.select(t, "partition", "offset", F.col("timestamp").alias("kafka_ts")).select(
        "t.symbol",
        "t.trade_id",
        F.col("t.price").try_cast(PRICE).alias("price"),
        F.col("t.qty").try_cast(PRICE).alias("qty"),
        F.timestamp_millis("t.trade_time_ms").alias("event_time"),
        F.col("t.is_buyer_maker"),
        F.timestamp_millis("t.ingested_at_ms").alias("produced_at"),
        "kafka_ts",
        "partition",
        "offset",
        F.to_date(F.timestamp_millis("t.trade_time_ms")).alias("trade_date"),
    )


def aggregate_1m(trades: DataFrame) -> DataFrame:
    """Trades -> 1-minute OHLCV candles per symbol (same shape as the batch klines)."""
    buy_qty = F.when(~F.col("is_buyer_maker"), F.col("qty")).otherwise(F.lit(0))
    return (
        trades.groupBy(F.window("event_time", "1 minute"), "symbol")
        .agg(
            F.min_by("price", F.struct("event_time", "trade_id")).alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", F.struct("event_time", "trade_id")).alias("close"),
            F.sum("qty").alias("volume"),
            F.sum(F.col("price") * F.col("qty")).cast(PRICE).alias("quote_volume"),
            F.count("*").alias("number_of_trades"),
            F.sum(buy_qty).alias("taker_buy_base_volume"),
            F.max("event_time").alias("last_trade_at"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "last_trade_at",
        )  # fmt: skip
    )


# --- sinks -----------------------------------------------------------------------------
def merge_candles(target_path: str):
    """foreachBatch handler: upsert one micro-batch of candles into Delta."""

    def _merge(batch: DataFrame, batch_id: int) -> None:
        from delta.tables import DeltaTable

        spark = batch.sparkSession
        batch = batch.withColumn("_batch_id", F.lit(batch_id)).withColumn(
            "_updated_at", F.current_timestamp()
        )
        if not DeltaTable.isDeltaTable(spark, target_path):
            batch.write.format("delta").save(target_path)
            return
        (
            DeltaTable.forPath(spark, target_path)
            .alias("t")
            .merge(batch.alias("s"), "t.symbol = s.symbol AND t.window_start = s.window_start")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return _merge


def main(argv: list[str] | None = None) -> int:
    from lakehouse.spark_session import get_spark

    ap = argparse.ArgumentParser(description="Kafka trades -> Delta (raw + 1-min candles)")
    ap.add_argument("--seconds", type=int, default=0, help="stop after N seconds (0 = forever)")
    ap.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = ap.parse_args(argv)

    settings = load_settings()
    spark: SparkSession = get_spark("stream-trades", with_kafka=True)
    bronze = settings.layer_path("bronze", True)
    silver = settings.layer_path("silver", True)
    ckpt = settings.layer_path("checkpoints", True)

    # Create the topic if the producer hasn't yet - otherwise starting this job first fails
    # with UnknownTopicOrPartitionException (start order shouldn't matter).
    from lakehouse.streaming.producer import ensure_topic

    ensure_topic(settings.kafka_bootstrap_servers)

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", TOPIC)
        .option("startingOffsets", args.starting_offsets)  # only used on the FIRST run
        .option("maxOffsetsPerTrigger", 200_000)  # back-pressure: cap each micro-batch
        .load()
    )
    trades = parse_trades(kafka)

    raw_q = (
        trades.writeStream.format("delta")
        .queryName("trades_raw")
        .outputMode("append")
        .partitionBy("trade_date")
        .option("checkpointLocation", f"{ckpt}/stream_trades_raw")
        .trigger(processingTime="10 seconds")
        .start(f"{bronze}/stream_trades")
    )

    candles_q = (
        aggregate_1m(trades.withWatermark("event_time", "2 minutes"))
        .writeStream.queryName("ohlcv_1m")
        .outputMode("update")  # emit only candles that changed in this micro-batch
        .foreachBatch(merge_candles(f"{silver}/stream_ohlcv_1m"))
        .option("checkpointLocation", f"{ckpt}/stream_ohlcv_1m")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("streaming... (Ctrl+C to stop)")
    started = time.time()
    try:
        while raw_q.isActive and candles_q.isActive:
            if args.seconds and time.time() - started > args.seconds:
                break
            time.sleep(10)
            p = raw_q.lastProgress
            if p:
                print(
                    f"[{p['timestamp'][11:19]}] batch {p['batchId']}: {p['numInputRows']} trades, "
                    f"{p.get('processedRowsPerSecond', 0):.0f} rows/s processed"
                )
    except KeyboardInterrupt:
        pass
    finally:
        summary = summarize(raw_q, candles_q, time.time() - started)
        for q in (raw_q, candles_q):
            q.stop()
        get_storage(settings, "silver").write_bytes(
            "_manifests/streaming_run.json", json.dumps(summary, indent=2).encode()
        )
        print(json.dumps(summary, indent=2))
        spark.stop()
    return 0


def summarize(raw_q, candles_q, seconds: float) -> dict:
    prog = [p for p in raw_q.recentProgress if p["numInputRows"] > 0]
    durations = sorted(p["durationMs"].get("triggerExecution", 0) for p in prog)
    rows = sum(p["numInputRows"] for p in prog)
    return {
        "run_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "seconds_running": round(seconds, 1),
        "micro_batches_with_data": len(prog),
        "trades_processed_recent": rows,
        "avg_trades_per_sec_ingested": round(rows / seconds, 1) if seconds else 0,
        "peak_processed_rows_per_sec": round(
            max((p.get("processedRowsPerSecond", 0) for p in prog), default=0), 1
        ),
        "median_batch_ms": durations[len(durations) // 2] if durations else None,
        "raw_query_exception": str(raw_q.exception()) if raw_q.exception() else None,
        "candles_query_exception": str(candles_q.exception()) if candles_q.exception() else None,
    }


if __name__ == "__main__":
    raise SystemExit(main())
