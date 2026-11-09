"""
Member 5 tests: Postgres load step and REST API.

Needs a running Postgres (docker compose up -d postgres). Skipped otherwise.
Uses a separate `fraud_test` database so real data is never touched.
"""

import os

import pytest

psycopg2 = pytest.importorskip("psycopg2")
pa = pytest.importorskip("pyarrow")
pytest.importorskip("fastapi")
import pyarrow.parquet as pq  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.serving.db import ensure_database, load_predictions  # noqa: E402

TEST_DB_URL = os.getenv(
    "FRAUD_TEST_DB_URL", "postgresql://airflow:airflow@localhost:5432/fraud_test"
)


@pytest.fixture
def db_url(monkeypatch):
    try:
        ensure_database(TEST_DB_URL)
    except psycopg2.OperationalError:
        pytest.skip("Postgres not reachable; run `docker compose up -d postgres`")
    conn = psycopg2.connect(TEST_DB_URL)
    with conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS fraud_predictions, pipeline_runs")
    conn.close()
    monkeypatch.setenv("FRAUD_DB_URL", TEST_DB_URL)
    return TEST_DB_URL


def write_predictions(path, rows):
    """Write a Spark-style output directory: part file plus _SUCCESS marker."""
    path.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys())
    table = pa.table({c: [r[c] for r in rows] for c in columns})
    pq.write_table(table, path / "part-00000.parquet")
    (path / "_SUCCESS").touch()
    return str(path)


def prediction(tx_id, probability, threshold=0.5):
    return {
        "transaction_id": tx_id,
        "type": "TRANSFER",
        "amount": 181.0,
        "step": 1,
        "fraud_probability": probability,
        "fraud_prediction": int(probability >= threshold),
        "model_threshold": threshold,
    }


def query(db_url, sql_text):
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            return cur.fetchall()
    finally:
        conn.close()


def test_load_upserts_and_records_run(db_url, tmp_path):
    src = write_predictions(tmp_path / "p", [prediction("a", 0.9), prediction("b", 0.1)])
    assert load_predictions(db_url, src, dag_run_id="run-1") == 2

    # Re-run with a changed score: row is updated, not duplicated
    src = write_predictions(tmp_path / "p", [prediction("a", 0.2), prediction("b", 0.1)])
    load_predictions(db_url, src, dag_run_id="run-2")

    rows = query(db_url, "SELECT transaction_id, fraud_probability, fraud_prediction "
                         "FROM fraud_predictions ORDER BY transaction_id")
    assert rows == [("a", 0.2, 0), ("b", 0.1, 0)]
    runs = query(db_url, "SELECT dag_run_id, status, rows_loaded FROM pipeline_runs ORDER BY run_id")
    assert runs == [("run-1", "success", 2), ("run-2", "success", 2)]


def test_load_handles_duplicate_ids_in_one_batch(db_url, tmp_path):
    src = write_predictions(tmp_path / "p", [prediction("a", 0.3), prediction("a", 0.8)])
    load_predictions(db_url, src)
    assert query(db_url, "SELECT fraud_probability FROM fraud_predictions") == [(0.8,)]


def test_load_failure_is_recorded(db_url, tmp_path):
    with pytest.raises(Exception):
        load_predictions(db_url, str(tmp_path / "missing"))
    assert query(db_url, "SELECT status FROM pipeline_runs") == [("failed",)]


def test_api_endpoints(db_url, tmp_path):
    from src.serving.api import app

    src = write_predictions(
        tmp_path / "p",
        [prediction("low", 0.1), prediction("mid", 0.6), prediction("high", 0.95)],
    )
    load_predictions(db_url, src)
    client = TestClient(app)

    assert client.get("/health").json() == {"status": "ok", "database": "ok"}

    tx = client.get("/transactions/high")
    assert tx.status_code == 200
    assert tx.json()["fraud_prediction"] == 1
    assert client.get("/transactions/nope").status_code == 404

    alerts = client.get("/alerts", params={"min_probability": 0.5}).json()
    assert [a["transaction_id"] for a in alerts] == ["high", "mid"]
    assert len(client.get("/alerts", params={"min_probability": 0.5, "limit": 1}).json()) == 1
    assert client.get("/alerts", params={"min_probability": 2}).status_code == 422


def test_api_returns_503_before_first_load(db_url):
    from src.serving.api import app

    client = TestClient(app)
    assert client.get("/health").status_code == 200
    response = client.get("/alerts")
    assert response.status_code == 503
    assert response.json() == {"detail": "No predictions loaded yet"}


def test_api_returns_503_when_db_down(monkeypatch):
    from src.serving.api import app

    monkeypatch.setenv("FRAUD_DB_URL", "postgresql://u:p@127.0.0.1:1/none")
    assert TestClient(app).get("/health").status_code == 503
