FROM python:3.12-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/riverwatch/src \
    PYSPARK_PYTHON=/usr/local/bin/python

RUN apt-get update \
    && apt-get install \
        --yes \
        --no-install-recommends \
        procps \
        tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd \
        --gid 1099 \
        spark \
    && useradd \
        --uid 1099 \
        --gid 1099 \
        --home-dir /home/spark \
        --create-home \
        --shell /bin/bash \
        spark

COPY containers/processing-spark-requirements.txt \
    /tmp/processing-spark-requirements.txt

RUN python -m pip install \
        --no-cache-dir \
        --upgrade pip \
    && python -m pip install \
        --no-cache-dir \
        -r /tmp/processing-spark-requirements.txt \
    && rm \
        /tmp/processing-spark-requirements.txt

RUN mkdir -p \
        /opt/riverwatch/src \
    && chown -R \
        1099:1099 \
        /opt/riverwatch

COPY --chown=1099:1099 \
    src/riverwatch \
    /opt/riverwatch/src/riverwatch

COPY --chown=1099:1099 \
    containers/processing_job.py \
    /opt/riverwatch/processing_job.py

USER spark