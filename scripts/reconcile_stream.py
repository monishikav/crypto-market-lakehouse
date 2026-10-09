"""Prove the real-time lane is correct: compare our streamed 1-minute candles with the
official candles from Binance's REST API for the same minutes.

    python scripts/reconcile_stream.py          # Windows venv (DuckDB reads the Delta table)

The first and last minute per symbol are skipped: the producer started/stopped mid-minute,
so those candles are partial by design.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from lakehouse.config import load_settings  # noqa: E402
from lakehouse.ingestion.http import http_get  # noqa: E402

settings = load_settings()
table = (settings.data_dir / "silver" / "stream_ohlcv_1m").as_posix()
con = duckdb.connect()
con.sql("SET TimeZone = 'UTC'")
streamed = con.sql(f"""
    with c as (
        select *, row_number() over (partition by symbol order by window_start) as rn,
                  count(*) over (partition by symbol) as n
        from delta_scan('{table}')
    )
    select symbol, epoch_ms(window_start) as open_ms, open, high, low, close, volume,
           number_of_trades
    from c where rn > 1 and rn < n
    order by symbol, open_ms
""").fetchall()
if not streamed:
    print("No complete streamed minutes yet - let the stream run for 3+ minutes.")
    raise SystemExit(1)

by_symbol: dict[str, list] = {}
for row in streamed:
    by_symbol.setdefault(row[0], []).append(row)

checked = exact = 0
worst = []
for symbol, rows in by_symbol.items():
    start, end = rows[0][1], rows[-1][1]
    url = (
        f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m"
        f"&startTime={start}&endTime={end}&limit=1000"
    )
    official = {k[0]: k for k in json.loads(http_get(url))}
    for _, open_ms, o, h, lo, c, v, n in rows:
        k = official.get(open_ms)
        if k is None:
            continue
        checked += 1
        ours = (o, h, lo, c, v, n)
        theirs = (Decimal(k[1]), Decimal(k[2]), Decimal(k[3]), Decimal(k[4]), Decimal(k[5]), k[8])
        if ours == theirs:
            exact += 1
        else:
            worst.append((symbol, open_ms, ours, theirs))

print(f"minutes compared: {checked} | exact OHLCV + trade-count match: {exact} "
      f"({100 * exact / max(checked, 1):.1f}%)")  # fmt: skip
for symbol, open_ms, ours, theirs in worst[:5]:
    print(f"  MISMATCH {symbol} {open_ms}: ours={ours} binance={theirs}")
