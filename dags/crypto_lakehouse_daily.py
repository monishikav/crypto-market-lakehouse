"""Daily batch pipeline: Binance day file -> bronze -> silver -> gold -> dbt.

    ingest_klines ─┐
                   ├─> silver_merge ─> gold_refresh_month ─> dbt_build
    ingest_metadata┘

Design choices worth knowing for interviews:

* Data intervals. Each run processes ONE closed UTC day: the run that fires at 02:00 UTC
  on Oct 8 processes Oct 7 ({{ data_interval_start | ds }}). Airflow 3 changed the
  default for cron schedules to "no data interval" (logical date = trigger time), so we
  opt back in explicitly with CronDataIntervalTimetable. The 2-hour delay gives Binance
  time to publish the previous day's file.
* Idempotency. Every task can be re-run for the same day with the same result:
  bronze skips files it already has, silver MERGEs on (symbol, open_time), gold swaps the
  whole month with replaceWhere, dbt rebuilds tables. That's what makes retries and
  backfills safe.
* Backfills. Re-processing any range of days is just:
      airflow backfill create --dag-id crypto_lakehouse_daily \
          --from-date 2026-10-01 --to-date 2026-10-07
  max_active_runs=1 makes backfill days run one after another, so two Spark jobs never
  write the same Delta table at the same time.
* Retries. A missing Binance file fails the ingest task (it's probably just not published
  yet) and Airflow retries with exponential backoff instead of loading a partial day.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

PROJECT = "/opt/project"
DAY = "{{ data_interval_start | ds }}"
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # 5 min, 10 min, 20 min ...
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="crypto_lakehouse_daily",
    description="Binance daily candles -> bronze -> silver (Delta MERGE) -> gold star -> dbt",
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="UTC"),
    start_date=datetime(2026, 10, 1),
    catchup=False,  # don't auto-run every day since start_date; use explicit backfills
    max_active_runs=1,
    default_args=default_args,
    tags=["crypto", "lakehouse", "batch"],
    doc_md=__doc__,
) as dag:
    ingest_klines = BashOperator(
        task_id="ingest_klines",
        bash_command=(
            "python -m lakehouse.ingestion.binance_bulk "
            f"--start {DAY} --end {DAY} --fail-on-missing"
        ),
        cwd=PROJECT,
    )

    ingest_metadata = BashOperator(
        task_id="ingest_metadata",
        bash_command="python -m lakehouse.ingestion.metadata",
        cwd=PROJECT,
    )

    silver_merge = BashOperator(
        task_id="silver_merge",
        bash_command=(
            "python -m lakehouse.transform.silver_klines "
            f"--mode incremental --start {DAY} --end {DAY}"
        ),
        cwd=PROJECT,
    )

    gold_refresh_month = BashOperator(
        task_id="gold_refresh_month",
        bash_command=f"python -m lakehouse.transform.gold_star --start {MONTH} --end {MONTH}",
        cwd=PROJECT,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --profiles-dir . --target dev",
        cwd=f"{PROJECT}/dbt",
        env={
            "LAKEHOUSE_GOLD": f"{PROJECT}/data/gold",
            "DBT_DUCKDB_PATH": f"{PROJECT}/data/crypto_lakehouse.duckdb",
        },
        append_env=True,  # keep PATH etc. - only add these two variables
    )

    [ingest_klines, ingest_metadata] >> silver_merge >> gold_refresh_month >> dbt_build
