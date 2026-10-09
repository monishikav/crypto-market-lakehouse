"""Phase 1 - Bronze: Binance historical candles (klines) from data.binance.vision.

    python -m lakehouse.ingestion.binance_bulk                     # everything in assets.yaml
    python -m lakehouse.ingestion.binance_bulk --symbols BTCUSDT --start 2025-01 --end 2025-01
    python -m lakehouse.ingestion.binance_bulk --start 2025-10-01 --end 2025-10-07   # daily files

Why the bulk files instead of the REST API? 10.5M candles via /api/v3/klines = ~10,500
paged requests with rate limits. Binance publishes the same data as one zip per
symbol per month (or per day): 240 downloads instead.

Bronze rules followed here:
  1. Land data *as received* - no parsing, no type casting, no fixes. If silver logic has a
     bug, we can always rebuild from bronze without re-downloading.
  2. Idempotent - re-running skips files already landed, so retries and backfills are safe.
  3. Verified - every zip is checked against Binance's published SHA-256 before landing.

We store each file as .csv.gz (not .zip) because Spark reads gzip natively but not zip.
The CSV bytes inside are untouched.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import time
import zipfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta

from tqdm import tqdm

from lakehouse.config import load_settings
from lakehouse.ingestion.http import NotFound, http_get
from lakehouse.storage import Storage, get_storage

BASE_URL = "https://data.binance.vision/data/spot"

# Binance spot kline CSV columns (no header row in the files). Documented here because
# silver will need them - bronze itself never parses the file.
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "number_of_trades", "taker_buy_base_volume",
    "taker_buy_quote_volume", "ignore",
]  # fmt: skip

Fetch = Callable[[str], bytes]


@dataclass
class FileResult:
    symbol: str
    period: str
    status: str  # downloaded | skipped | missing | failed
    rows: int = 0
    zip_bytes: int = 0
    stored_bytes: int = 0
    key: str = ""
    error: str = ""


# --- periods ---------------------------------------------------------------------------
def build_periods(start: str, end: str) -> list[str]:
    """'2025-01'..'2025-03' -> monthly periods; '2025-10-01'..'2025-10-03' -> daily."""
    if len(start) != len(end) or len(start) not in (7, 10):
        raise ValueError("use YYYY-MM for both (monthly) or YYYY-MM-DD for both (daily)")
    if len(start) == 7:
        y, m = map(int, start.split("-"))
        end_y, end_m = map(int, end.split("-"))
        periods = []
        while (y, m) <= (end_y, end_m):
            periods.append(f"{y:04d}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    else:
        d, last = date.fromisoformat(start), date.fromisoformat(end)
        periods = []
        while d <= last:
            periods.append(d.isoformat())
            d += timedelta(days=1)
    if not periods:
        raise ValueError(f"empty range {start}..{end}")
    return periods


def file_name(symbol: str, interval: str, period: str) -> str:
    return f"{symbol}-{interval}-{period}"


def source_url(symbol: str, interval: str, period: str) -> str:
    freq = "monthly" if len(period) == 7 else "daily"
    return f"{BASE_URL}/{freq}/klines/{symbol}/{interval}/{file_name(symbol, interval, period)}.zip"


def bronze_key(symbol: str, interval: str, period: str) -> str:
    # Hive-style key=value folders: Spark/Athena turn these into columns for free.
    return (
        f"binance/klines/interval={interval}/symbol={symbol}/"
        f"{file_name(symbol, interval, period)}.csv.gz"
    )


# --- one file --------------------------------------------------------------------------
def verify_checksum(zip_bytes: bytes, checksum_text: str) -> None:
    expected = checksum_text.split()[0].strip().lower()
    actual = hashlib.sha256(zip_bytes).hexdigest()
    if actual != expected:
        raise ValueError(f"SHA-256 mismatch: expected {expected[:12]}…, got {actual[:12]}…")


def zip_to_gzip(zip_bytes: bytes) -> tuple[bytes, int]:
    """Unzip the single CSV inside and re-compress it as gzip. Returns (gz_bytes, rows)."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if len(csv_names) != 1:
            raise ValueError(f"expected 1 CSV in zip, found {csv_names}")
        raw = zf.read(csv_names[0])
    rows = raw.count(b"\n") + (0 if raw.endswith(b"\n") or not raw else 1)
    # mtime=0 -> identical input gives byte-identical output (deterministic, diff-able)
    return gzip.compress(raw, compresslevel=6, mtime=0), rows


def ingest_file(
    symbol: str, interval: str, period: str, storage: Storage, fetch: Fetch = http_get
) -> FileResult:
    key = bronze_key(symbol, interval, period)
    if storage.exists(key):
        return FileResult(symbol, period, "skipped", key=key)

    url = source_url(symbol, interval, period)
    try:
        checksum = fetch(url + ".CHECKSUM").decode()
        zip_bytes = fetch(url)
    except NotFound:
        # e.g. a coin that listed mid-year, or a day Binance hasn't published yet
        return FileResult(symbol, period, "missing", key=key)

    verify_checksum(zip_bytes, checksum)
    gz_bytes, rows = zip_to_gzip(zip_bytes)
    storage.write_bytes(key, gz_bytes)
    return FileResult(symbol, period, "downloaded", rows, len(zip_bytes), len(gz_bytes), key)


# --- the whole run ---------------------------------------------------------------------
def run(
    symbols: list[str],
    interval: str,
    periods: list[str],
    storage: Storage,
    workers: int = 8,
    fetch: Fetch = http_get,
    show_progress: bool = True,
) -> dict:
    started = time.perf_counter()
    jobs = [(s, p) for s in symbols for p in periods]
    results: list[FileResult] = []

    # Downloads are I/O-bound (waiting on the network), so threads give a near-linear
    # speed-up even with Python's GIL.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(ingest_file, s, interval, p, storage, fetch): (s, p) for s, p in jobs
        }
        bar = tqdm(total=len(jobs), unit="file", disable=not show_progress)
        for fut in as_completed(futures):
            s, p = futures[fut]
            try:
                results.append(fut.result())
            except Exception as exc:  # one bad file must not kill the other 239
                results.append(FileResult(s, p, "failed", error=f"{type(exc).__name__}: {exc}"))
            bar.update(1)
        bar.close()

    results.sort(key=lambda r: (r.symbol, r.period))
    count = {
        st: sum(r.status == st for r in results)
        for st in ("downloaded", "skipped", "missing", "failed")
    }
    manifest = {
        "run_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "interval": interval,
        "periods": [periods[0], periods[-1]],
        "symbols": len(symbols),
        "files": count,
        "rows_downloaded": sum(r.rows for r in results),
        "zip_mb_downloaded": round(sum(r.zip_bytes for r in results) / 1e6, 1),
        "stored_mb": round(sum(r.stored_bytes for r in results) / 1e6, 1),
        "seconds": round(time.perf_counter() - started, 1),
        "results": [asdict(r) for r in results],
    }
    # The manifest is our audit trail: what was loaded, when, and how much.
    stamp = manifest["run_at_utc"].replace(":", "").replace("-", "")
    storage.write_bytes(
        f"_manifests/binance_klines/run_{stamp}.json", json.dumps(manifest, indent=2).encode()
    )
    return manifest


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    ap = argparse.ArgumentParser(description="Land Binance klines into the bronze layer")
    ap.add_argument("--symbols", help="comma-separated, default: all in assets.yaml")
    ap.add_argument("--start", default=settings.start_month, help="YYYY-MM or YYYY-MM-DD")
    ap.add_argument("--end", default=settings.end_month, help="YYYY-MM or YYYY-MM-DD")
    ap.add_argument("--interval", default=settings.interval)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="exit 1 if any file is not published yet (daily runs: let Airflow retry)",
    )
    args = ap.parse_args(argv)

    symbols = args.symbols.split(",") if args.symbols else settings.symbols
    periods = build_periods(args.start, args.end)
    storage = get_storage(settings, "bronze")
    target = storage.uri("binance/klines")
    print(f"Landing {len(symbols)} symbols x {len(periods)} periods -> {target}")

    m = run(symbols, args.interval, periods, storage, workers=args.workers)

    f = m["files"]
    print(
        f"\nfiles: {f['downloaded']} downloaded, {f['skipped']} skipped (already landed), "
        f"{f['missing']} missing, {f['failed']} failed"
    )
    print(f"rows downloaded: {m['rows_downloaded']:,} | zip: {m['zip_mb_downloaded']} MB | "
          f"stored gz: {m['stored_mb']} MB | {m['seconds']}s")  # fmt: skip
    for r in m["results"]:
        if r["status"] in ("missing", "failed"):
            print(f"  {r['status'].upper():8} {r['symbol']} {r['period']} {r['error']}")
    if f["failed"] or (args.fail_on_missing and f["missing"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
