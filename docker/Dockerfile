# One image for the whole stack: Airflow 3 (orchestration) + Java + PySpark + Delta
# (batch and streaming jobs) + dbt (in its own venv). Using the same image for Airflow and
# for ad-hoc Spark runs means "works when I run it by hand" == "works when Airflow runs it".
FROM apache/airflow:3.2.2-python3.11

USER root
# Spark runs on the JVM. default-jre-headless gives Java 17 or 21, both supported by Spark 4.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
ARG PY_MM=3.11
COPY docker/requirements-docker.txt /tmp/requirements-docker.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements-docker.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_MM}.txt"

# dbt pins many of the same libraries as Airflow, so it gets an isolated venv.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-duckdb==1.11.0
ENV PATH="${PATH}:/home/airflow/dbt-venv/bin"

# Pre-download the Delta + Kafka-connector JARs at build time so jobs start offline/fast.
COPY docker/warmup_spark.py /tmp/warmup_spark.py
RUN SPARK_LOCAL_IP=127.0.0.1 python /tmp/warmup_spark.py

ENV PYTHONPATH=/opt/project/src
WORKDIR /opt/project
