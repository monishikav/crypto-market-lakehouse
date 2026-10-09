"""Central configuration.

Why this module exists: every job needs the same answers to "which coins?", "where is
the bronze layer?", "are we on local disk or S3?". Putting those answers in one place
means a job never hard-codes a path, and switching local -> S3 is a single env var.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

# src/lakehouse/config.py -> parents[2] is the repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

LAYERS = ("bronze", "silver", "gold", "checkpoints")


@dataclass(frozen=True)
class Asset:
    symbol: str
    coingecko_id: str


@dataclass(frozen=True)
class Settings:
    assets: tuple[Asset, ...]
    interval: str
    start_month: str
    end_month: str
    streaming_symbols: tuple[str, ...]
    storage_mode: str  # "local" or "s3"
    data_dir: Path
    s3_bucket: str
    aws_region: str
    kafka_bootstrap_servers: str

    @property
    def symbols(self) -> list[str]:
        return [a.symbol for a in self.assets]

    def layer_path(self, layer: str, for_spark: bool = False) -> str:
        """Root URI of a lakehouse layer.

        local -> <repo>/data/bronze
        s3    -> s3://bucket/bronze  (Spark needs the s3a:// scheme, boto3 needs bucket+key)
        """
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; expected one of {LAYERS}")
        if self.storage_mode == "s3":
            scheme = "s3a" if for_spark else "s3"
            return f"{scheme}://{self.s3_bucket}/{layer}"
        return (self.data_dir / layer).as_posix()


def load_settings(assets_file: Path | None = None) -> Settings:
    assets_file = assets_file or PROJECT_ROOT / "config" / "assets.yaml"
    raw = yaml.safe_load(assets_file.read_text(encoding="utf-8"))

    storage_mode = os.getenv("STORAGE_MODE", "local").strip().lower()
    if storage_mode not in {"local", "s3"}:
        raise ValueError("STORAGE_MODE must be 'local' or 's3'")
    s3_bucket = os.getenv("S3_BUCKET", "").strip()
    if storage_mode == "s3" and not s3_bucket:
        raise ValueError("STORAGE_MODE=s3 requires S3_BUCKET in .env")

    data_dir = Path(os.getenv("DATA_DIR", "data"))
    if not data_dir.is_absolute():
        data_dir = PROJECT_ROOT / data_dir

    return Settings(
        assets=tuple(Asset(**a) for a in raw["assets"]),
        interval=raw["history"]["interval"],
        start_month=str(raw["history"]["start_month"]),
        end_month=str(raw["history"]["end_month"]),
        streaming_symbols=tuple(raw["streaming"]["symbols"]),
        storage_mode=storage_mode,
        data_dir=data_dir,
        s3_bucket=s3_bucket,
        aws_region=os.getenv("AWS_REGION", "ap-south-1"),
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
