"""Phase 0 health check - run from the repo root inside your venv:

    python scripts/check_setup.py

Prints OK / WARN / FAIL for everything the project needs. Paste the output back to
your guide if anything is not OK. Uses only the standard library so it runs even when
the venv install is half-finished.
"""

from __future__ import annotations

import importlib
import os
import platform
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
results: list[tuple[str, str, str]] = []


def record(status: str, name: str, detail: str = "") -> None:
    results.append((status, name, detail))


def run(cmd: list[str], timeout: int = 20) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or p.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)


def total_ram_gb() -> float | None:
    try:
        if sys.platform == "win32":
            import ctypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
            return stat.ullTotalPhys / 1024**3
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3
    except Exception:
        return None


# 1. Python + venv -------------------------------------------------------------------
v = sys.version_info
py = f"{v.major}.{v.minor}.{v.micro}"
if (3, 11) <= (v.major, v.minor) <= (3, 13):
    record("OK", "Python", py)
elif (v.major, v.minor) >= (3, 10):
    record("WARN", "Python", f"{py} (3.11-3.13 recommended)")
else:
    record("FAIL", "Python", f"{py} - need 3.10+")

in_venv = sys.prefix != sys.base_prefix
record("OK" if in_venv else "WARN", "Virtualenv active", sys.prefix if in_venv else "not in a venv")

# 2. Host packages -------------------------------------------------------------------
packages = {
    "requests": "requests",
    "dotenv": "python-dotenv",
    "yaml": "pyyaml",
    "tqdm": "tqdm",
    "boto3": "boto3",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "duckdb": "duckdb",
    "websocket": "websocket-client",
    "confluent_kafka": "confluent-kafka",
    "dbt.version": "dbt-core",
    "dbt.adapters.duckdb": "dbt-duckdb",
    "pytest": "pytest",
}
missing = []
for module, dist in packages.items():
    try:
        importlib.import_module(module)
    except Exception:
        missing.append(dist)
if missing:
    record("FAIL", "Python packages", "missing: " + ", ".join(missing))
else:
    from importlib.metadata import version

    record("OK", "Python packages", f"{len(packages)} installed (dbt-core {version('dbt-core')})")

try:
    sys.path.insert(0, str(ROOT / "src"))
    from lakehouse.config import load_settings

    s = load_settings()
    record(
        "OK",
        "Project config",
        f"{len(s.assets)} assets, storage={s.storage_mode}, bronze={s.layer_path('bronze')}",
    )
except Exception as exc:
    record("FAIL", "Project config", repr(exc))

record(
    "OK" if (ROOT / ".env").exists() else "WARN",
    ".env file",
    "found" if (ROOT / ".env").exists() else "copy .env.example to .env",
)

# 3. Tooling -------------------------------------------------------------------------
for tool in ("git",):
    code, out = run([tool, "--version"])
    record("OK" if code == 0 else "FAIL", tool, out.splitlines()[0] if code == 0 else "not found")

if shutil.which("docker"):
    code, out = run(["docker", "version", "--format", "{{.Server.Version}}"])
    record(
        "OK" if code == 0 else "FAIL",
        "Docker engine",
        f"server {out}" if code == 0 else "Docker Desktop not running - start it",
    )
    code, out = run(["docker", "compose", "version", "--short"])
    record("OK" if code == 0 else "FAIL", "Docker Compose", out if code == 0 else "missing")
    code, out = run(["docker", "info", "--format", "{{.MemTotal}}"])
    if code == 0 and out.isdigit():
        gb = int(out) / 1024**3
        record(
            "OK" if gb >= 6 else "WARN",
            "Docker memory",
            f"{gb:.1f} GB" + ("" if gb >= 6 else " - give Docker/WSL 6+ GB"),
        )
else:
    record("FAIL", "Docker", "not installed - install Docker Desktop (WSL2 backend)")

ram = total_ram_gb()
if ram:
    record(
        "OK" if ram >= 12 else "WARN",
        "Machine RAM",
        f"{ram:.1f} GB" + ("" if ram >= 12 else " - run Kafka/Airflow one at a time"),
    )

if shutil.which("aws"):
    code, out = run(["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"])
    record(
        "OK" if code == 0 else "WARN",
        "AWS CLI",
        f"account {out}" if code == 0 else "installed but not configured (aws configure)",
    )
else:
    record("WARN", "AWS CLI", "not installed - fine for now (local mode); needed in Phase 3")

# 4. Network: can we reach the data sources? -----------------------------------------
endpoints = {
    "Binance bulk data": "https://data.binance.vision/?prefix=data/spot/monthly/klines/BTCUSDT/1m/",
    "Binance REST API": "https://api.binance.com/api/v3/ping",
    "CoinGecko API": "https://api.coingecko.com/api/v3/ping",
}
for name, url in endpoints.items():
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "crypto-lakehouse-setup"})
        with urllib.request.urlopen(req, timeout=10) as r:
            record("OK", name, f"HTTP {r.status}")
    except Exception as exc:
        record("WARN", name, f"unreachable ({exc.__class__.__name__}: {exc})"[:110])

# Report -----------------------------------------------------------------------------
print(f"\nCrypto Market Lakehouse - setup check ({platform.system()} {platform.release()})\n")
width = max(len(n) for _, n, _ in results)
for status, name, detail in results:
    print(f"  [{status:<4}] {name:<{width}}  {detail}")
fails = sum(s == "FAIL" for s, _, _ in results)
warns = sum(s == "WARN" for s, _, _ in results)
print(
    f"\n{fails} fail, {warns} warn. "
    + ("Ready for Phase 1!" if fails == 0 else "Fix the FAIL lines first.")
)
sys.exit(1 if fails else 0)
