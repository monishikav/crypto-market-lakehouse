# Crypto Market Lakehouse

![CI](https://github.com/<your-github-username>/crypto-market-lakehouse/actions/workflows/ci.yml/badge.svg)

End-to-end **batch + streaming** data platform for 20 crypto assets, built on a medallion
(bronze → silver → gold) lakehouse with Delta Lake, modelled with dbt, orchestrated by
Airflow, with a real-time lane through Kafka and Spark Structured Streaming.

```
                ┌──────────────────── batch · daily 02:00 UTC · Airflow ───────────────────┐
 Binance bulk ─►│ bronze            silver                 gold                  dbt        │
 CoinGecko API ►│ raw CSV/JSON ──► Delta, typed, deduped ─► star schema ──────► 15 models  │──► DuckDB
                │ SHA-256 checked   quality-quarantined     fact + 3 dims        39 tests    │
                └──────────────────────────────────────────────────────────────────────────┘
 Binance live ──► Kafka (3 partitions, key=symbol) ──► Spark Structured Streaming ──► Delta
 trades (WS)       idempotent producer                 1-min windows, 2-min watermark   MERGE upserts
```

## Tech stack
Python 3.11 · PySpark 4.0 · Delta Lake 4.0 · Apache Kafka 4.2 (KRaft) · Apache Airflow 3.2 ·
dbt 1.12 · DuckDB · Streamlit · Docker Compose · GitHub Actions · pytest · ruff

## Measured results
Laptop: Windows, 16 GB RAM, Spark in local mode inside Docker.

| Metric | Result |
|---|---|
| Candles in the lakehouse | **18,374,400** one-minute candles (20 coins × 21 months, Jan 2025 – Sep 2026) |
| Bronze ingestion | 240 monthly files in **85 s** (8 parallel downloads), every file SHA-256 verified, 0 failures |
| Data completeness | 7,300 coin-days checked by dbt, **0 missing minutes** |
| Silver build | full: 200 s for 10.5M rows · incremental MERGE of 9 months (7.9M rows): 219 s · re-runs leave row counts unchanged (idempotent) |
| Bytes scanned, raw CSV → partitioned Delta | 1 coin, 1 day: **−96%** · 20 coins, 1 month: **−98%** (402 → 8 MB, 9.4 s → 1.4 s) · full-year aggregate: **−87%** |
| Gold star schema | fact_prices + dim_coin / dim_date / dim_exchange · **0 orphan keys** · sorting within partitions cut size **36%** vs silver |
| dbt | **15 models · 39 data tests · 2 enforced contracts** (schema drift fails the build) |
| Failure handling | disk-full crash mid-MERGE left **0 corrupted rows**: Delta never committed the write |
| Streaming | **52,664** live trades (BTC/ETH/SOL) → Kafka at 175 msg/s, **0 delivery failures** · Spark Structured Streaming processed **2,914 rows/s** (~16× headroom over the live rate) · Kafka → Delta count matched exactly (**0 lost, 0 duplicated**) · after a stop/restart the job resumed from its checkpoint and added only the 37,426 new trades (90,090 total = 52,664 + 37,426: exactly-once across restarts) |

Sanity check against real market events: the dbt marts show **10 Oct 2025** as the worst day
for 11 of 20 coins (−23% to −27%, the October 2025 liquidation crash) and **2 Mar 2025** as
the best day for ADA (+72%), XRP (+34%) and SOL (+24%).

## Project structure
```
crypto-market-lakehouse/
├── config/assets.yaml            # 20 coins, history window, streaming symbols
├── src/lakehouse/
│   ├── config.py                 # paths per layer; STORAGE_MODE=local|s3
│   ├── storage.py                # local / S3 writes, atomic temp-file + rename
│   ├── spark_session.py          # Spark + Delta (+ Kafka connector), UTC session
│   ├── ingestion/                # bronze: Binance bulk klines, CoinGecko + exchangeInfo
│   ├── transform/                # silver_klines.py (clean, dedupe, DQ, MERGE) · gold_star.py
│   └── streaming/                # producer.py (WS → Kafka) · trades_to_delta.py (Kafka → Delta)
├── dbt/                          # staging → intermediate → marts + quality; contracts
├── dags/crypto_lakehouse_daily.py
├── dashboard/app.py              # Streamlit dashboard: scorecard, coin explorer, patterns, live candles
├── scripts/                      # setup checks, scan benchmark, Delta VACUUM, stream reconciliation, CI fixture
├── tests/                        # 24 pytest tests (ingestion, Spark transforms, streaming, DAG)
├── docker/ · docker-compose.yml  # one image: Airflow + Java + Spark + Delta + dbt; Kafka (KRaft)
└── .github/workflows/ci.yml      # lint → tests + dbt build on fixture → DAG integrity
```

## Run it
Prerequisites: Python 3.11, Docker Desktop, Git. ~15 GB free disk.

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt; pip install -e .
Copy-Item .env.example .env
docker compose build

# Batch: bronze → silver → gold → dbt
python -m lakehouse.ingestion.binance_bulk
python -m lakehouse.ingestion.metadata
docker compose run --rm spark python -m lakehouse.transform.silver_klines
docker compose run --rm spark python -m lakehouse.transform.gold_star
cd dbt; $env:LAKEHOUSE_GOLD = (Resolve-Path ..\data\gold).Path -replace '\\','/'; dbt build --profiles-dir .; cd ..

# Dashboard over the dbt marts: http://localhost:8501
streamlit run dashboard/app.py

# Orchestration: http://localhost:8090, unpause crypto_lakehouse_daily
docker compose up -d airflow

# Streaming (two terminals)
docker compose up -d kafka
python -m lakehouse.streaming.producer --seconds 360
docker compose run --rm --service-ports spark python -m lakehouse.streaming.trades_to_delta --seconds 300
python scripts\reconcile_stream.py
```

## Design decisions
| Decision | Why |
|---|---|
| Bronze stores files exactly as received (gzip only) | Any silver bug is fixed by re-processing, never by re-downloading |
| Idempotent everything (skip-if-exists, MERGE, replaceWhere) | Airflow retries and backfills can re-run any day safely |
| `DECIMAL(28,8)` for prices, not float | Exact money arithmetic; floats drift when summed |
| Detect ms vs µs timestamps per row | Binance switched spot files to microseconds in 2025 |
| Partition silver by symbol + **month**, not day | Day partitions = 7,300 tiny files (small-files problem); month = 240 × ~1.5 MB |
| Quarantine bad rows instead of dropping them | Data-quality failures stay visible and countable |
| Session time zone pinned to UTC (Spark and DuckDB) | Local IST would shift candles near midnight into the wrong day/month |
| xxhash64 surrogate keys | Deterministic across runs; no lookup joins for 18M fact rows |
| `CronDataIntervalTimetable` in Airflow 3 | Each run processes one closed UTC day (`data_interval_start`) |
| Streaming candles by event time with a 2-min watermark | Late trades still counted; state can't grow forever |
| dbt contracts on the main marts | A type change upstream fails CI instead of silently breaking BI |

## CI
`.github/workflows/ci.yml` runs on every push and PR:
1. **lint**: ruff check + format
2. **test-and-dbt**: pytest (Spark on Java 17), builds a synthetic gold layer with the real
   gold code, then `dbt build` runs all 15 models, 39 tests and the contracts
3. **airflow-dag**: imports the DAG under Airflow 3.2.2 and checks its wiring and schedule

To make it a merge gate: GitHub → Settings → Branches → add a rule for `main` →
*Require status checks to pass* → select the three jobs.
