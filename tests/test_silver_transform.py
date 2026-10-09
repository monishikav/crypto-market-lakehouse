"""Silver transform tests. Need PySpark + Java, so they are skipped in the Windows venv and
run in Docker (pytest isn't baked into the image, so install it on the fly) or in CI:
    docker compose run --rm spark bash -c "pip install -q pytest && python -m pytest tests"
"""

import datetime as dt
import gzip
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from lakehouse.transform import silver_klines as sk  # noqa: E402

# 2025-01-01 00:00 UTC in micros, and 2024-12-31 23:59 UTC in millis (old format).
US = 1735689600000000
MS = 1735689540000


def line(open_time, o, h, low, c, v="1.5", close_offset=59_999):
    close_time = open_time + (close_offset * 1000 if open_time >= 10**15 else close_offset)
    return f"{open_time},{o},{h},{low},{c},{v},{close_time},100.0,10,0.5,50.0,0"


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def write_bronze(root, symbol, period, lines):
    d = root / "binance/klines/interval=1m" / f"symbol={symbol}"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{symbol}-1m-{period}.csv.gz").write_bytes(gzip.compress("\n".join(lines).encode()))


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("bronze")
    write_bronze(root, "BTCUSDT", "2025-01", [
        line(US, "93576.00", "93610.93", "93537.50", "93610.93"),          # good (micros)
        line(US + 60_000_000, "100", "90", "95", "96"),                    # high < low -> bad
        line(US + 120_000_000, "-1", "10", "-2", "5"),                     # negative price
        line(US + 180_000_000, "abc", "10", "9", "9.5"),                   # unparseable
    ])  # fmt: skip
    write_bronze(root, "BTCUSDT", "2025-01-01", [
        line(US, "93576.00", "93610.93", "93537.50", "93610.93"),          # duplicate of row 1
    ])  # fmt: skip
    write_bronze(root, "ETHUSDT", "2024-12", [
        line(MS, "3330.10", "3335.00", "3329.00", "3334.20", v="12.3"),    # good (millis)
    ])  # fmt: skip
    raw = sk.read_bronze(spark, root.as_posix(), "1m")
    clean, rejected = sk.build_silver(raw)
    return raw, clean.collect(), {r["_dq_error"] for r in rejected.collect()}


def test_reads_partition_column_and_source_file(silver):
    raw, _, _ = silver
    assert {r.symbol for r in raw.select("symbol").collect()} == {"BTCUSDT", "ETHUSDT"}
    assert raw.count() == 6


def test_micro_and_milli_timestamps_normalise_to_same_clock(silver):
    _, clean, _ = silver
    by_sym = {r.symbol: r for r in clean}
    assert by_sym["BTCUSDT"].open_time == dt.datetime(2025, 1, 1, 0, 0)
    assert by_sym["BTCUSDT"]._ts_unit == "us"
    assert by_sym["ETHUSDT"].open_time == dt.datetime(2024, 12, 31, 23, 59)
    assert by_sym["ETHUSDT"]._ts_unit == "ms"


def test_partition_columns_are_utc_based(silver):
    _, clean, _ = silver
    eth = next(r for r in clean if r.symbol == "ETHUSDT")
    # 23:59 UTC on Dec 31 is 05:29 IST on Jan 1 - must stay in December.
    assert eth.trade_date == dt.date(2024, 12, 31) and eth.trade_month == "2024-12"


def test_money_is_decimal_not_float(silver):
    _, clean, _ = silver
    btc = next(r for r in clean if r.symbol == "BTCUSDT")
    assert btc.close == Decimal("93610.93000000")


def test_bad_rows_quarantined_with_reason(silver):
    _, _, reasons = silver
    assert reasons == {"high_below_ohlc", "non_positive_price", "null_price"}


def test_duplicates_removed(silver):
    _, clean, _ = silver
    assert len(clean) == 2  # 6 raw - 3 rejected - 1 duplicate
