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
| `GET /health` | `{"status": "ok", "database": "ok"}`, or 503 if Postgres is down. Creates the `fraud` database if it does not exist yet |
| `GET /transactions/{transaction_id}` | One prediction, or 404 |
| `GET /alerts?limit=100` | Transactions the model flagged (`fraud_prediction = 1`, i.e. at `model_threshold`, currently 0.4), highest probability first |
| `GET /alerts?min_probability=0.9&limit=100` | Predictions with `fraud_probability >= min_probability`, highest first. `min_probability` must be 0–1, `limit` 1–1000 |

Before the first load step has run, `/transactions` and `/alerts` return
503 `{"detail": "No predictions loaded yet"}`.

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

## Verified with real data

On 2026-11-09, `predict_fraud.py` was run on the current Gold sample (10 rows),
then loaded and queried through the API container. Schema, load and all endpoints
matched the contract (10 rows loaded, 1 flagged at probability 0.416).

Scale check with 1M synthetic predictions (local Docker Postgres): first load
35 s, full re-load (upsert) 50 s, so the full 6.3M PaySim set takes about
4–5 minutes. `/alerts` queries use the probability index (under 2 ms).

## Airflow integration

Task `member5_load_predictions` in `airflow/dags/fraud_detection_pipeline_dag.py`
runs `scripts/load_predictions_to_postgres.py` after `member3_fraud_detection`:

```
ingest >> etl >> quality >> inference >> member5_load_predictions >> health_check >> success
```

The Airflow containers get `FRAUD_DB_URL` from `docker-compose.yml`, and the
Airflow image ships `psycopg2`. The Airflow `run_id` is stored in `pipeline_runs.dag_run_id`.

## Verified end to end in Airflow

On 2026-11-09, `airflow dags test fraud_detection_pipeline` ran in the compose stack
(Postgres, Kafka, API, Airflow image built from `airflow/Dockerfile`). All 7 tasks
succeeded: ingestion, ETL, quality checks, inference, `member5_load_predictions`,
health check, success. Postgres then held 10 predictions, `pipeline_runs` logged the
run as `success`, and `/alerts` returned the flagged transaction (0.416).

The run used `dataset/sample_paysim.csv` copied to `dataset/paysim.csv`. The real
PaySim file (~493 MB) is not in git: download it to `dataset/paysim.csv` first.

`rows_loaded` can be larger than the row count in `fraud_predictions`: the DAG appends
to Bronze and Silver, so Gold repeats transaction ids across runs. The upsert keeps one
row per `transaction_id`.

## Team fixes made on this branch

- **Spark version:** the model in `models/member3/` was saved with Spark 4.2.0, and
  on Spark 3.5 it fails to load (`UNRESOLVED_COLUMN ... treeID`). `airflow/Dockerfile`
  now pins `pyspark==4.2.0`.
- **Airflow image build:** the base image is Debian bullseye, whose apt repos are
  retired. Java 17 is now copied from `eclipse-temurin:17-jre` instead of apt.
- **pyarrow:** `pyarrow>=14` pulled 26.x, which needs NumPy 2; Airflow 2.7.2 ships
  NumPy 1.24, so `import pyarrow` failed. Capped at `<20`.
- **DAG `PROJECT_ROOT`:** it went up three folders, which is `/opt` in the container,
  so every task failed with `can't open file '/opt/scripts/...'`. It now picks the
  first parent folder that contains `scripts/`.
- **DAG ETL input format:** the ETL task passed `--format parquet`, but the input is
  the PaySim CSV, so Spark failed with `CANNOT_READ_FILE_FOOTER`. Now `--format csv`.
- **File permissions:** Spark chmods the files it writes, and fails on files that came
  from the host (`data/` is in git): `chmod: changing permissions of
  '.../_SUCCESS': Operation not permitted`. Airflow containers now run as
  `${AIRFLOW_UID:-0}:0` (root by default). On Linux, `AIRFLOW_UID=$(id -u)` keeps
  outputs owned by your user.
- **Batch ETL import:** `src/etl/batch_etl.py` imports `LakehouseTelemetry`, which
  commit `9e71fa3` removed. The class is restored in `src/utils/lakehouse_telemetry.py`.
  Member 2's PR #3 adds a fallback class in `batch_etl.py`, but its line 95 still has the
  old bare import, so PR #3 alone does not fix the `ImportError`.
- **Tests:** the `spark_session` and lakehouse fixtures lost from `tests/conftest.py`
  in the `member2-pr` merge are restored from the `member2` branch.
- **Known limit:** streaming ETL loads `spark-sql-kafka-0-10_2.12:3.5.0`
  (`src/lakehouse/spark_session.py`), which does not match Spark 4. The DAG only runs
  batch ETL, so it is not affected.

## Verified on the full PaySim dataset

On 2026-11-10 the whole DAG ran on the real PaySim CSV (6,362,620 rows, 8,213 frauds).
All 7 tasks succeeded in about 24 minutes.

| Stage | Result |
|-------|--------|
| Bronze / Silver / Gold | 6,362,620 rows each |
| Inference | 7,598 transactions flagged at threshold 0.4 |
| Postgres load | 6,362,620 rows in 3 min 29 s, run logged as `success` |
| `/alerts` | Returns the flagged transactions, highest probability (0.997) first |

Against the `isFraud` label: 5,938 true positives, 1,660 false positives, 2,275 missed
frauds. Precision 0.782, recall 0.723, F1 0.751. This set includes the model's training
rows, so the held-out test scores (precision 0.773, recall 0.726, F1 0.749) are the fair numbers.

**Memory:** the run needs more than the default Docker Desktop memory on a 16 GB laptop.
At 7.6 GB, with other containers running, the Docker VM ran out of memory and killed
every container. It passed with `memory=11GB` and `swap=8GB` in `%USERPROFILE%\.wslconfig`.
The Airflow scheduler peaked at 2.9 GiB.
