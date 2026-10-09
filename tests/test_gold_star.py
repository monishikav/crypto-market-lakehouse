"""Gold star-schema tests (PySpark; skipped in the Windows venv, run in Docker / CI)."""

import datetime as dt
import json
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from lakehouse.config import load_settings  # noqa: E402
from lakehouse.transform import gold_star as gs  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("gold-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


@pytest.fixture(scope="module")
def bronze(tmp_path_factory):
    root = tmp_path_factory.mktemp("bronze")
    for day, rank in (("2026-10-07", 2), ("2026-10-08", 1)):  # two snapshots: latest wins
        cg = root / f"coingecko/coins_markets/snapshot_date={day}/coins_markets.json"
        cg.parent.mkdir(parents=True)
        cg.write_text(json.dumps([
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap": 1.6e12,
             "market_cap_rank": rank, "max_supply": 21000000.0, "ath": 126080,
             "ath_date": "2025-10-06T10:57:42.000Z", "image": "x"},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum", "market_cap": 5e11,
             "market_cap_rank": 2, "max_supply": None, "ath": 4950.5, "image": "y"},
        ]))  # fmt: skip
        bx = root / f"binance/exchange_info/snapshot_date={day}/exchange_info.json"
        bx.parent.mkdir(parents=True)
        bx.write_text(json.dumps({"timezone": "UTC", "symbols": [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING"},
            {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT", "status": "TRADING"},
        ]}))  # fmt: skip
    return root.as_posix()


def test_dim_date(spark):
    d = gs.build_dim_date(spark, "2024-12-30", "2025-01-05").collect()
    assert len(d) == 7
    first = d[0]
    assert first.date_key == 20241230 and first.day_name == "Monday" and not first.is_weekend
    assert sum(r.is_weekend for r in d) == 2  # Sat 4th + Sun 5th


def test_dim_coin_uses_latest_snapshot_and_all_assets(spark, bronze):
    settings = load_settings()
    cg, bx = gs.read_coingecko(spark, bronze), gs.read_exchange_info(spark, bronze)
    dim = {r.symbol: r for r in gs.build_dim_coin(spark, settings, cg, bx).collect()}
    assert len(dim) == 20  # every configured asset gets a row, even without metadata
    btc = dim["BTCUSDT"]
    assert btc.coin_name == "Bitcoin" and btc.market_cap_rank == 1  # 10-08, not 10-07
    assert btc.snapshot_date == dt.date(2026, 10, 8) and btc.has_supply_cap
    assert dim["ETHUSDT"].has_supply_cap is False
    assert dim["SOLUSDT"].coin_name is None  # missing metadata -> NULLs, not a lost coin
    assert len({r.coin_key for r in dim.values()}) == 20


def test_fact_keys_match_dimensions(spark, bronze):
    silver = spark.createDataFrame(
        [("BTCUSDT", dt.datetime(2025, 1, 1, 0, 0), dt.date(2025, 1, 1), "2025-01",
          Decimal("1"), Decimal("2"), Decimal("0.5"), Decimal("1.5"), Decimal("3"),
          Decimal("4.5"), 10, Decimal("1"))],
        "symbol string, open_time timestamp, trade_date date, trade_month string, "
        "open decimal(28,8), high decimal(28,8), low decimal(28,8), close decimal(28,8), "
        "volume decimal(28,8), quote_asset_volume decimal(28,8), number_of_trades long, "
        "taker_buy_base_volume decimal(28,8)",
    )  # fmt: skip
    fact = gs.build_fact_prices(silver)
    dims = {
        "dim_date": gs.build_dim_date(spark, "2025-01-01", "2025-01-01"),
        "dim_coin": gs.build_dim_coin(
            spark,
            load_settings(),
            gs.read_coingecko(spark, bronze),
            gs.read_exchange_info(spark, bronze),
        ),  # fmt: skip
        "dim_exchange": gs.build_dim_exchange(spark),
    }
    assert gs.integrity_checks(fact, dims) == {
        "orphan_coin_key": 0, "orphan_date_key": 0, "orphan_exchange_key": 0,
        "duplicate_coin_keys": 0,
    }  # fmt: skip
    row = fact.first()
    assert row.date_key == 20250101 and row.quote_volume_usdt == Decimal("4.5")
