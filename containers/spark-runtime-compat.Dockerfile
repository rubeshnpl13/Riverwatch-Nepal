FROM eclipse-temurin:21-jre-jammy AS java

FROM python:3.12-slim-bookworm

COPY --from=java \
    /opt/java/openjdk \
    /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    PYTHONPATH=/workspace/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=/usr/local/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

WORKDIR /workspace

COPY src ./src

RUN python -m pip install \
        --no-cache-dir \
        "pyspark==4.0.1" \
        "pytest>=8.3,<9.0" \
        "pydantic>=2.9,<3.0" \
        "pydantic-settings>=2.5,<3.0" \
        "httpx>=0.27,<1.0" \
        "tenacity>=9.0,<10.0" \
        duckdb \
        fastapi \
        uvicorn \
        "google-cloud-storage>=3.0,<4.0" \
        "google-cloud-pubsub>=2.42,<3.0" \
        "google-cloud-dataproc>=5.30,<6.0" \
        "google-cloud-bigquery>=3.30,<4.0"