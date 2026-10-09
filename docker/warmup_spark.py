"""Build-time step: start Spark once with Delta + Kafka packages so their JARs land in the
image's Ivy cache. Kept self-contained (no project imports) because the repo is mounted
only at runtime."""

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.master("local[1]")
    .appName("warmup")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
spark = configure_spark_with_delta_pip(builder, extra_packages=[kafka_pkg]).getOrCreate()
spark.range(5).write.format("delta").mode("overwrite").save("/tmp/warmup_delta")
assert spark.read.format("delta").load("/tmp/warmup_delta").count() == 5
print(f"Spark {spark.version} + Delta + Kafka connector cached OK")
spark.stop()
