FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

ARG REQUIREMENTS_FILE=requirements.txt

COPY requirements.txt requirements-runtime.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r "${REQUIREMENTS_FILE}"

COPY app ./app
COPY scripts ./scripts
COPY pytest.ini .
COPY README.md .

RUN mkdir -p /app/storage /app/logs /app/runtime \
    && adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app

USER appuser

CMD ["sh", "-c", "python -m uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --proxy-headers --forwarded-allow-ips ${CRYPTO_FORWARDED_ALLOW_IPS:-168.144.35.241}"]
