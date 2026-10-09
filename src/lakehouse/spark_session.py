"""One SparkSession factory for every Spark job (batch + streaming).

Runs inside the Docker image (Java + PySpark + Delta are installed there), not in your
Windows venv. Key ideas:
- Delta Lake is a Spark *extension*: two configs turn plain Parquet folders into ACID
  tables with MERGE, time travel and schema enforcement.
- configure_spark_with_delta_pip() adds the matching Delta JAR from Maven, so the
  Python package version and the JVM jar can never drift apart.
- Session timezone is pinned to UTC: crypto trades 24/7 and Binance timestamps are UTC
  epoch millis; a local timezone would silently shift daily partitions.
"""

from __future__ import annotations

import os


def get_spark(app_name: str = "crypto-lakehouse", with_kafka: bool = False):
    import pyspark
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        # Default is 200 shuffle partitions - tuned for clusters, far too many on a laptop.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.showConsoleProgress", "false")
    )

    extra_packages: list[str] = []
    if with_kafka:
        # Kafka source for Structured Streaming; version must match Spark exactly.
        extra_packages.append(f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}")

    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
