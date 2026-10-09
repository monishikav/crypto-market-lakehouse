from pathlib import Path

import pytest

from lakehouse.config import load_settings


def test_assets_loaded_and_unique():
    s = load_settings()
    assert len(s.assets) == 20
    assert len(set(s.symbols)) == len(s.symbols), "duplicate symbol in assets.yaml"
    assert all(sym.endswith("USDT") for sym in s.symbols)


def test_streaming_symbols_are_subset_of_assets():
    s = load_settings()
    assert set(s.streaming_symbols) <= set(s.symbols)


def test_local_layer_paths(monkeypatch):
    monkeypatch.setenv("STORAGE_MODE", "local")
    s = load_settings()
    assert Path(s.layer_path("bronze")).name == "bronze"


def test_s3_layer_paths(monkeypatch):
    monkeypatch.setenv("STORAGE_MODE", "s3")
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    s = load_settings()
    assert s.layer_path("silver") == "s3://my-bucket/silver"
    assert s.layer_path("silver", for_spark=True) == "s3a://my-bucket/silver"


def test_unknown_layer_rejected():
    with pytest.raises(ValueError):
        load_settings().layer_path("platinum")
