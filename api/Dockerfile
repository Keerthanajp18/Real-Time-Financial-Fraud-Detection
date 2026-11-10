FROM python:3.11-slim

WORKDIR /app

# Member 5 API dependencies only (no Spark/pyarrow needed to serve)
RUN pip install --no-cache-dir \
    "fastapi>=0.110" \
    "uvicorn>=0.29" \
    "psycopg2-binary>=2.9"

# src/ is mounted by docker-compose, so code changes need no rebuild
CMD ["uvicorn", "src.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
