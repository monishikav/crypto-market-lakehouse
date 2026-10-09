"""Offline tests for Phase 1: a fake Binance serves in-memory zips, so no network needed."""

import gzip
import hashlib
import io
import json
import zipfile

import pytest

from lakehouse.ingestion import binance_bulk as bb
from lakehouse.ingestion.http import NotFound
from lakehouse.ingestion.metadata import fetch_binance_exchange_info, fetch_coingecko_markets
from lakehouse.storage import LocalStorage

# Two real-looking 1m candles. Since 2025 Binance spot files use MICROsecond timestamps.
CSV = (
    b"1735689600000000,93576.00,93610.93,93537.50,93610.93,8.21827,1735689659999999,"
    b"768978.43,1730,4.89377,457925.29,0\n"
    b"1735689660000000,93610.93,93652.00,93606.15,93650.01,4.69127,1735689719999999,"
    b"439329.57,1185,2.53911,237784.75,0\n"
)


def make_zip(name: str, payload: bytes = CSV) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(f"{name}.csv", payload)
    return buf.getvalue()


class FakeBinance:
    """Serves a zip + .CHECKSUM for each (symbol, period) in `available`."""

    def __init__(self, available, corrupt=()):
        self.files, self.calls = {}, []
        for symbol, period in available:
            name = bb.file_name(symbol, "1m", period)
            z = make_zip(name)
            url = bb.source_url(symbol, "1m", period)
            digest = hashlib.sha256(z).hexdigest()
            if (symbol, period) in corrupt:
                digest = "0" * 64
            self.files[url] = z
            self.files[url + ".CHECKSUM"] = f"{digest}  {name}.zip".encode()

    def __call__(self, url, headers=None):
        self.calls.append(url)
        if url not in self.files:
            raise NotFound(url)
        return self.files[url]


def test_build_periods_monthly_and_daily():
    assert bb.build_periods("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]
    assert bb.build_periods("2025-02-27", "2025-03-01") == [
        "2025-02-27",
        "2025-02-28",
        "2025-03-01",
    ]
    with pytest.raises(ValueError):
        bb.build_periods("2025-01", "2025-01-05")


def test_urls_and_keys():
    assert bb.source_url("BTCUSDT", "1m", "2025-01").endswith(
        "/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-01.zip"
    )
    assert "/spot/daily/klines/" in bb.source_url("BTCUSDT", "1m", "2025-01-05")
    assert bb.bronze_key("ETHUSDT", "1m", "2025-03") == (
        "binance/klines/interval=1m/symbol=ETHUSDT/ETHUSDT-1m-2025-03.csv.gz"
    )


def test_ingest_lands_gzip_with_identical_bytes(tmp_path):
    storage = LocalStorage(tmp_path)
    fake = FakeBinance([("BTCUSDT", "2025-01")])
    r = bb.ingest_file("BTCUSDT", "1m", "2025-01", storage, fetch=fake)
    assert r.status == "downloaded" and r.rows == 2
    landed = gzip.decompress(storage.read_bytes(r.key))
    assert landed == CSV  # bronze = exactly what the source sent
    assert not list(tmp_path.rglob("*.tmp"))  # atomic write left no temp files


def test_rerun_is_idempotent(tmp_path):
    storage = LocalStorage(tmp_path)
    fake = FakeBinance([("BTCUSDT", "2025-01")])
    bb.ingest_file("BTCUSDT", "1m", "2025-01", storage, fetch=fake)
    n_calls = len(fake.calls)
    again = bb.ingest_file("BTCUSDT", "1m", "2025-01", storage, fetch=fake)
    assert again.status == "skipped"
    assert len(fake.calls) == n_calls  # no network on the second run


def test_checksum_mismatch_is_rejected(tmp_path):
    storage = LocalStorage(tmp_path)
    fake = FakeBinance([("BTCUSDT", "2025-01")], corrupt=[("BTCUSDT", "2025-01")])
    with pytest.raises(ValueError, match="SHA-256"):
        bb.ingest_file("BTCUSDT", "1m", "2025-01", storage, fetch=fake)
    assert not storage.exists(bb.bronze_key("BTCUSDT", "1m", "2025-01"))


def test_run_reports_missing_and_failed_without_crashing(tmp_path):
    storage = LocalStorage(tmp_path)
    fake = FakeBinance(
        [("BTCUSDT", "2025-01"), ("BTCUSDT", "2025-02"), ("ETHUSDT", "2025-01")],
        corrupt=[("ETHUSDT", "2025-01")],
    )
    m = bb.run(["BTCUSDT", "ETHUSDT"], "1m", ["2025-01", "2025-02"], storage,
               workers=4, fetch=fake, show_progress=False)  # fmt: skip
    assert m["files"] == {"downloaded": 2, "skipped": 0, "missing": 1, "failed": 1}
    assert m["rows_downloaded"] == 4
    manifests = list((tmp_path / "_manifests" / "binance_klines").glob("run_*.json"))
    assert len(manifests) == 1 and json.loads(manifests[0].read_text())["files"]["failed"] == 1


def test_metadata_validates_ids():
    cg = json.dumps([{"id": "bitcoin"}]).encode()
    with pytest.raises(ValueError, match="ethereum"):
        fetch_coingecko_markets(["bitcoin", "ethereum"], fetch=lambda url, headers=None: cg)
    assert fetch_coingecko_markets(["bitcoin"], fetch=lambda url, headers=None: cg) == cg

    bx = json.dumps({"symbols": [{"symbol": "BTCUSDT"}]}).encode()
    seen = []

    def fake(url, headers=None):
        seen.append(url)
        return bx

    assert fetch_binance_exchange_info(["BTCUSDT"], fetch=fake) == bx
    assert "symbols=%5B%22BTCUSDT%22%5D" in seen[0]  # JSON array, URL-encoded
