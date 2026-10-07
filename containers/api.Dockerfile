FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080

WORKDIR /app

RUN addgroup \
      --system \
      riverwatch \
    && adduser \
      --system \
      --ingroup riverwatch \
      riverwatch

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install \
      --no-cache-dir \
      --upgrade pip \
    && python -m pip install \
      --no-cache-dir \
      .

USER riverwatch

EXPOSE 8080

CMD ["sh", "-c", "exec python -m uvicorn riverwatch.cloud.api_app:create_cloud_api_app --factory --host 0.0.0.0 --port ${PORT}"]