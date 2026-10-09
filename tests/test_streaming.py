"""Streaming lane tests. The producer test runs anywhere; the Spark tests reuse the exact
transforms the streaming job uses, but on a static DataFrame (same code, batch mode)."""

import datetime as dt
import json
from decimal import Decimal

import pytest

from lakehouse.streaming.producer import to_record

T0 = 1_759_881_600_000  # 2025-10-08 00:00:00 UTC in ms


def ws_msg(trade_id, price, qty, t_ms, maker=False, symbol="BTCUSDT"):
    return json.dumps({"stream": f"{symbol.lower()}@trade", "data": {
        "e": "trade", "E": t_ms + 5, "s": symbol, "t": trade_id, "p": price, "q": qty,
        "T": t_ms, "m": maker, "M": True,
    }})  # fmt: skip


def test_producer_maps_binance_trade_to_keyed_record():
    key, value = to_record(ws_msg(1, "62000.10", "0.5", T0))
    assert key == "BTCUSDT"  # key = symbol -> per-symbol ordering in Kafka
    assert value["price"] == "62000.10" and value["trade_time_ms"] == T0
    assert to_record(json.dumps({"data": {"e": "aggTrade"}})) is None


pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from lakehouse.streaming.trades_to_delta import aggregate_1m, parse_trades  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def kafka_rows(spark, msgs):
    rows = []
    for i, m in enumerate(msgs):
        key, value = to_record(m)
        rows.append((key.encode(), json.dumps(value).encode(), 0, i, dt.datetime(2025, 10, 8)))
    return spark.createDataFrame(
        rows, "key binary, value binary, partition int, offset long, timestamp timestamp"
    )


def test_one_minute_candles(spark):
    msgs = [
        ws_msg(1, "100.0", "1", T0 + 1_000),             # 00:00 open
        ws_msg(2, "105.0", "2", T0 + 20_000, maker=True),  # high; seller-initiated
        ws_msg(3, "99.0", "1", T0 + 40_000),             # low
        ws_msg(4, "101.0", "1", T0 + 59_999),            # 00:00 close
        ws_msg(5, "102.0", "3", T0 + 60_000),            # 00:01 candle
        ws_msg(6, "50.0", "1", T0 + 2_000, symbol="ETHUSDT"),
    ]  # fmt: skip
    trades = parse_trades(kafka_rows(spark, msgs))
    candles = {(r.symbol, r.window_start.minute): r for r in aggregate_1m(trades).collect()}

    c = candles[("BTCUSDT", 0)]
    assert (c.open, c.high, c.low, c.close) == tuple(
        Decimal(x) for x in ("100", "105", "99", "101")
    )
    assert c.volume == Decimal(5) and c.number_of_trades == 4
    assert c.taker_buy_base_volume == Decimal(3)  # trade 2 was a taker SELL
    assert c.quote_volume == Decimal("100") + Decimal("210") + Decimal("99") + Decimal("101")
    assert candles[("BTCUSDT", 1)].open == Decimal("102")
    assert candles[("ETHUSDT", 0)].close == Decimal("50")


def test_event_time_comes_from_trade_not_arrival(spark):
    t = parse_trades(kafka_rows(spark, [ws_msg(1, "1", "1", T0 + 61_000)])).first()
    assert t.event_time == dt.datetime(2025, 10, 8, 0, 1, 1)
    assert t.trade_date == dt.date(2025, 10, 8)
