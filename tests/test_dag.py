"""DAG integrity tests: the DAG must import cleanly and be wired the way we think.
Skipped where Airflow isn't installed (the Windows venv); runs in Docker and CI.
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.dags["crypto_lakehouse_daily"]  # in-memory; get_dag() would query the metadata DB


def test_structure(dag):
    assert set(dag.task_ids) == {
        "ingest_klines", "ingest_metadata", "silver_merge", "gold_refresh_month", "dbt_build",
    }  # fmt: skip
    assert dag.get_task("silver_merge").upstream_task_ids == {"ingest_klines", "ingest_metadata"}
    assert dag.get_task("dbt_build").upstream_task_ids == {"gold_refresh_month"}


def test_safe_defaults(dag):
    assert dag.max_active_runs == 1 and dag.catchup is False
    for t in dag.tasks:
        assert t.retries >= 3 and t.execution_timeout is not None


def test_each_run_processes_the_previous_utc_day(dag):
    import pendulum

    run_at = pendulum.datetime(2026, 10, 8, 2, 0, tz="UTC")
    interval = dag.timetable.infer_manual_data_interval(run_after=run_at)
    assert interval.start == pendulum.datetime(2026, 10, 7, 2, 0, tz="UTC")
    assert interval.start.strftime("%Y-%m-%d") == "2026-10-07"  # -> {{ data_interval_start | ds }}
