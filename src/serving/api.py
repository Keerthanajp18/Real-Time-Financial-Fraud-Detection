"""
Member 5: REST API over the fraud predictions stored in Postgres.

Run locally:  uvicorn src.serving.api:app --reload
Docs:         http://localhost:8000/docs
"""

import json
import os
from pathlib import Path
from typing import Optional

import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from psycopg2.errors import UndefinedTable
from psycopg2.extras import RealDictCursor

from src.serving.db import DEFAULT_DB_URL, PREDICTION_COLUMNS, ensure_database

app = FastAPI(title="Fraud Detection API", version="1.0.0")


@app.exception_handler(UndefinedTable)
def no_predictions_yet(request, exc):
    # Tables are created by the first load step run
    return JSONResponse(status_code=503, content={"detail": "No predictions loaded yet"})

_SELECT = "SELECT {}, loaded_at FROM fraud_predictions".format(", ".join(PREDICTION_COLUMNS))


def get_conn():
    # shortcut: one connection per request, no pool; add psycopg2.pool if traffic grows
    db_url = os.getenv("FRAUD_DB_URL", DEFAULT_DB_URL)
    try:
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor, connect_timeout=5)
    except psycopg2.OperationalError:
        try:
            # Fresh Postgres: the database does not exist until the first load, so create it
            ensure_database(db_url)
            conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor, connect_timeout=5)
        except psycopg2.OperationalError:
            raise HTTPException(status_code=503, detail="Database unavailable")
    try:
        yield conn
    finally:
        conn.close()


DASHBOARD = Path(__file__).parent / "static" / "index.html"


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(DASHBOARD)


@app.get("/health")
def health(conn=Depends(get_conn)):
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok", "database": "ok"}


@app.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str, conn=Depends(get_conn)):
    with conn.cursor() as cur:
        cur.execute(_SELECT + " WHERE transaction_id = %s", (transaction_id,))
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return row


@app.get("/alerts")
def get_alerts(
    min_probability: Optional[float] = Query(None, ge=0.0, le=1.0),
    limit: int = Query(100, ge=1, le=1000),
    conn=Depends(get_conn),
):
    # Without min_probability, use the model's own threshold (fraud_prediction = 1)
    if min_probability is None:
        where, params = "fraud_prediction = 1", (limit,)
    else:
        where, params = "fraud_probability >= %s", (min_probability, limit)
    with conn.cursor() as cur:
        cur.execute(
            _SELECT + f" WHERE {where} "
            "ORDER BY fraud_probability DESC, transaction_id LIMIT %s",
            params,
        )
        return cur.fetchall()


# Relative to the working directory: /app in the API container, the repo root locally
MODEL_CONFIG_PATH = "models/member3/model_config.json"


@app.get("/stats")
def get_stats(conn=Depends(get_conn)):
    """Totals, flagged counts per type, recent load runs and model metrics, for the dashboard."""
    with conn.cursor() as cur:
        # One scan of fraud_predictions: about 0.4 s on the 6.36M-row PaySim set
        cur.execute(
            "SELECT type, count(*) AS total, sum(fraud_prediction)::int AS flagged "
            "FROM fraud_predictions GROUP BY type ORDER BY flagged DESC, type"
        )
        by_type = cur.fetchall()
        cur.execute(
            "SELECT run_id, dag_run_id, status, rows_loaded, error, started_at, finished_at "
            "FROM pipeline_runs ORDER BY run_id DESC LIMIT 5"
        )
        runs = cur.fetchall()
    try:
        with open(MODEL_CONFIG_PATH, encoding="utf-8") as f:
            config = json.load(f)
        model = {k: config.get(k) for k in
                 ("model_type", "threshold", "test_precision", "test_recall", "test_f1")}
    except (OSError, ValueError):
        model = None  # API started without the models/ mount
    return {
        "total": sum(r["total"] for r in by_type),
        "flagged": sum(r["flagged"] for r in by_type),
        "by_type": by_type,
        "runs": runs,
        "model": model,
        "airflow_url": os.getenv("AIRFLOW_UI_URL", "http://localhost:8081"),
    }
