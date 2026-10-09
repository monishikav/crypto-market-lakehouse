"""Phase 1 - Bronze: reference data for the dimension tables.

    python -m lakehouse.ingestion.metadata

Two small daily snapshots, landed as raw JSON exactly as the APIs return them:
  - CoinGecko /coins/markets  -> dim_coin   (name, rank, market cap, supply, ATH, ...)
  - Binance /exchangeInfo     -> dim_exchange / symbol attributes (base, quote, status)

Snapshots are keyed by snapshot_date=YYYY-MM-DD. Re-running on the same day overwrites
that day's snapshot (idempotent); different days accumulate history - which is what lets
us build slowly-changing dimensions later (e.g. how a coin's rank changed over time).
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from urllib.parse import quote

from lakehouse.config import load_settings
from lakehouse.ingestion.http import http_get
from lakehouse.storage import get_storage

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
BINANCE_EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo"


def fetch_coingecko_markets(coin_ids: list[str], fetch=http_get) -> bytes:
    url = (
        f"{COINGECKO_URL}?vs_currency=usd&ids={','.join(coin_ids)}"
        "&order=market_cap_desc&per_page=250&page=1&sparkline=false"
        "&price_change_percentage=24h,7d,30d"
    )
    key = os.getenv("COINGECKO_API_KEY", "").strip()
    headers = {"x-cg-demo-api-key": key} if key else None
    body = fetch(url) if headers is None else fetch(url, headers=headers)
    returned = {c["id"] for c in json.loads(body)}
    missing = sorted(set(coin_ids) - returned)
    if missing:  # wrong id in assets.yaml -> fail loudly now, not as NULLs in a dashboard
        raise ValueError(f"CoinGecko returned no data for: {missing}")
    return body


def fetch_binance_exchange_info(symbols: list[str], fetch=http_get) -> bytes:
    url = f"{BINANCE_EXCHANGE_INFO_URL}?symbols={quote(json.dumps(symbols, separators=(',', ':')))}"
    body = fetch(url)
    returned = {s["symbol"] for s in json.loads(body)["symbols"]}
    missing = sorted(set(symbols) - returned)
    if missing:
        raise ValueError(f"Binance exchangeInfo missing: {missing}")
    return body


def main() -> int:
    settings = load_settings()
    storage = get_storage(settings, "bronze")
    snapshot = datetime.now(UTC).date().isoformat()

    cg = fetch_coingecko_markets([a.coingecko_id for a in settings.assets])
    cg_key = f"coingecko/coins_markets/snapshot_date={snapshot}/coins_markets.json"
    storage.write_bytes(cg_key, cg)
    print(f"CoinGecko: {len(json.loads(cg))} coins -> {storage.uri(cg_key)}")

    bx = fetch_binance_exchange_info(settings.symbols)
    bx_key = f"binance/exchange_info/snapshot_date={snapshot}/exchange_info.json"
    storage.write_bytes(bx_key, bx)
    print(
        f"Binance exchangeInfo: {len(json.loads(bx)['symbols'])} symbols -> {storage.uri(bx_key)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
