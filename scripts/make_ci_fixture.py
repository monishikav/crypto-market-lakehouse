"""Build a small, synthetic gold layer for CI (no downloads, ~1 minute).

    python scripts/make_ci_fixture.py ci_fixture

Uses the project's REAL gold builders (gold_star.py) on random-walk prices for 3 coins x
40 days, then writes plain Parquet. CI runs `dbt build --target ci` against it, so every
dbt model, test and contract is exercised on every pull request.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import random
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lakehouse.config import load_settings  # noqa: E402
from lakehouse.transform import gold_star as gs  # noqa: E402

out = Path(sys.argv[1] if len(sys.argv) > 1 else "ci_fixture")
gold, bronze = out / "gold", out / "bronze"
settings = load_settings()
spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

# Metadata snapshots in the same shape the real APIs return.
day = "2026-01-01"
coins = [("bitcoin", "btc", "Bitcoin", 1, 21e6), ("ethereum", "eth", "Ethereum", 2, None),
         ("solana", "sol", "Solana", 6, None)]  # fmt: skip
cg = bronze / f"coingecko/coins_markets/snapshot_date={day}/coins_markets.json"
cg.parent.mkdir(parents=True, exist_ok=True)
cg.write_text(json.dumps([
    {"id": i, "symbol": s, "name": n, "market_cap_rank": r, "market_cap": 1e11,
     "max_supply": m, "ath": 1.0, "ath_date": "2025-01-01T00:00:00.000Z", "image": ""}
    for i, s, n, r, m in coins
]))  # fmt: skip
bx = bronze / f"binance/exchange_info/snapshot_date={day}/exchange_info.json"
bx.parent.mkdir(parents=True, exist_ok=True)
bx.write_text(json.dumps({"symbols": [
    {"symbol": s, "baseAsset": s[:-4], "quoteAsset": "USDT", "status": "TRADING"}
    for s in settings.symbols
]}))  # fmt: skip

# Random-walk minute candles that satisfy every silver quality rule.
random.seed(42)
rows, start, days = [], dt.datetime(2025, 1, 1), 40
for sym, price in (("BTCUSDT", 93_500.0), ("ETHUSDT", 3_340.0), ("SOLUSDT", 190.0)):
    for m in range(days * 1440):
        t = start + dt.timedelta(minutes=m)
        o, price = price, max(0.01, price * math.exp(random.gauss(0, 0.0009)))
        hi = max(o, price) * (1 + abs(random.gauss(0, 3e-4)))
        lo = min(o, price) * (1 - abs(random.gauss(0, 3e-4)))
        v = abs(random.gauss(5, 2))
        rows.append((sym, t, t.date(), t.strftime("%Y-%m"), f"{o:.8f}", f"{hi:.8f}",
                     f"{lo:.8f}", f"{price:.8f}", f"{v:.8f}", f"{v * price:.8f}",
                     random.randint(50, 900), f"{v / 2:.8f}"))  # fmt: skip

money = ["open", "high", "low", "close", "volume", "quote_asset_volume", "taker_buy_base_volume"]
silver = spark.createDataFrame(
    rows,
    "symbol string, open_time timestamp, trade_date date, trade_month string, open string, "
    "high string, low string, close string, volume string, quote_asset_volume string, "
    "number_of_trades long, taker_buy_base_volume string",
)
silver = silver.select(
    *[F.col(c).cast("decimal(28,8)") if c in money else c for c in silver.columns]
)

gs.build_fact_prices(silver).repartition("trade_month").write.mode("overwrite").partitionBy(
    "trade_month"
).parquet(str(gold / "fact_prices"))
last = (start + dt.timedelta(days=days - 1)).date().isoformat()
gs.build_dim_date(spark, "2025-01-01", last).write.mode("overwrite").parquet(str(gold / "dim_date"))
gs.build_dim_coin(
    spark,
    settings,
    gs.read_coingecko(spark, str(bronze)),
    gs.read_exchange_info(spark, str(bronze)),
).write.mode("overwrite").parquet(str(gold / "dim_coin"))
gs.build_dim_exchange(spark).write.mode("overwrite").parquet(str(gold / "dim_exchange"))
print(f"CI fixture: {len(rows):,} minute candles -> {gold.resolve()}")
spark.stop()
