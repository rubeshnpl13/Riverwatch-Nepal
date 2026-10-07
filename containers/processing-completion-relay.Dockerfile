FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    PORT=8080

WORKDIR /app

RUN addgroup \
      --system \
      riverwatch \
    && adduser \
      --system \
      --ingroup riverwatch \
      riverwatch

COPY containers/processing-completion-relay-requirements.txt \
    /tmp/processing-completion-relay-requirements.txt

RUN python -m pip install \
      --no-cache-dir \
      --upgrade pip \
    && python -m pip install \
      --no-cache-dir \
      -r /tmp/processing-completion-relay-requirements.txt \
    && rm \
      /tmp/processing-completion-relay-requirements.txt

RUN mkdir -p /app/src

COPY --chown=riverwatch:riverwatch \
    src/riverwatch \
    /app/src/riverwatch

USER riverwatch

EXPOSE 8080

CMD ["sh", "-c", "exec python -m uvicorn riverwatch.cloud.processing_completion_relay_app:create_app --factory --host 0.0.0.0 --port ${PORT}"]