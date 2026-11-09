# Member 5: Serving Layer (Postgres + REST API)

Loads Member 3's fraud predictions into Postgres and serves them over a REST API.

```
data/gold/fraud_predictions (Parquet)
        │  scripts/load_predictions_to_postgres.py
        ▼
Postgres, database `fraud`  ──►  FastAPI (src/serving/api.py)  ──►  http://localhost:8000
```

## Files

| File | Purpose |
|------|---------|
| `src/serving/db.py` | Table schema, database creation, Parquet → Postgres upsert |
| `src/serving/api.py` | FastAPI app |
| `scripts/load_predictions_to_postgres.py` | CLI load step (used by Airflow) |
| `api/Dockerfile` | API container image |
| `tests/test_serving.py` | Load and API tests (need Postgres) |

## Database

Uses the existing `postgres` container from `docker-compose.yml`, in a separate
database named `fraud` so the tables stay apart from Airflow's metadata.
The load step creates the database and tables if they do not exist.

**`fraud_predictions`**: one row per transaction, primary key `transaction_id`.
Columns follow the Member 3 contract: `transaction_id, type, amount, step,
fraud_probability, fraud_prediction, model_threshold`, plus `loaded_at`.
Re-loading the same transaction updates the row; it does not duplicate it.
`isFraud` is not stored: it is for evaluation only.

**`pipeline_runs`**: one row per load: `run_id, dag_run_id, source_path,
status (running/success/failed), rows_loaded, error, started_at, finished_at`.

The whole load runs in one transaction. If it fails, `fraud_predictions` is
unchanged and the run is recorded as `failed` with the error message.

Connection string: env var `FRAUD_DB_URL`
(default `postgresql://airflow:airflow@localhost:5432/fraud`).

## API

| Endpoint | Returns |
|----------|---------|
| `GET /health` | `{"status": "ok", "database": "ok"}`, or 503 if Postgres is down |
| `GET /transactions/{transaction_id}` | One prediction, or 404 |
| `GET /alerts?min_probability=0.5&limit=100` | Predictions with `fraud_probability >= min_probability`, highest first. `min_probability` must be 0–1, `limit` 1–1000 |

Interactive docs: http://localhost:8000/docs

## How to run

```bash
pip install -r requirements.txt

# 1. Start Postgres and the API
docker compose up -d postgres api

# 2. Produce predictions (Member 3), then load them
python scripts/predict_fraud.py
python scripts/load_predictions_to_postgres.py

# 3. Query
curl http://localhost:8000/alerts?min_probability=0.9
```

## Tests

```bash
docker compose up -d postgres
pytest tests/test_serving.py -v
```

The tests use a separate `fraud_test` database and skip if Postgres is not
reachable. Set `FRAUD_TEST_DB_URL` to point them at another server.

**Port conflict:** if a native PostgreSQL is installed on Windows, it also
listens on 5432 and rejects the `airflow` password. Either stop that service, or
run a test container on another port:

```bash
docker run -d --rm --name m5-test-pg -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow -p 55432:5432 postgres:15
FRAUD_TEST_DB_URL=postgresql://airflow:airflow@localhost:55432/fraud_test pytest tests/test_serving.py -v
```

## Airflow integration (to agree with Member 4)

The Airflow containers already get `FRAUD_DB_URL` from `docker-compose.yml`, and
the Airflow image already ships `psycopg2`. The DAG change is not made yet.
Proposed task, after `member3_fraud_detection`:

```python
def run_member5_load(**context):
    """Member 5: Load fraud predictions into Postgres for the API"""
    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "load_predictions_to_postgres.py"),
        "--input-path", "data/gold/fraud_predictions",
        "--run-id", context["run_id"],
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        logger.error(f"Load failed: {result.stderr}")
        raise Exception("Member 5 load failed")
    logger.info(result.stdout)
    return {"status": "success", "step": "serving_load"}

# inside `with dag:`
    load_task = PythonOperator(
        task_id='member5_load_predictions',
        python_callable=run_member5_load,
        doc='Load fraud predictions into Postgres for the API',
    )

ingest_task >> etl_task >> quality_task >> inference_task >> load_task >> health_check_task >> success_task
```
