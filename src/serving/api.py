"""
Member 5: REST API over the fraud predictions stored in Postgres.

Run locally:  uvicorn src.serving.api:app --reload
Docs:         http://localhost:8000/docs
"""

import os

import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor

from src.serving.db import DEFAULT_DB_URL, PREDICTION_COLUMNS

app = FastAPI(title="Fraud Detection API", version="1.0.0")

_SELECT = "SELECT {}, loaded_at FROM fraud_predictions".format(", ".join(PREDICTION_COLUMNS))


def get_conn():
    # shortcut: one connection per request, no pool; add psycopg2.pool if traffic grows
    try:
        conn = psycopg2.connect(
            os.getenv("FRAUD_DB_URL", DEFAULT_DB_URL),
            cursor_factory=RealDictCursor,
            connect_timeout=5,
        )
    except psycopg2.OperationalError:
        raise HTTPException(status_code=503, detail="Database unavailable")
    try:
        yield conn
    finally:
        conn.close()


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
    min_probability: float = Query(0.5, ge=0.0, le=1.0),
    limit: int = Query(100, ge=1, le=1000),
    conn=Depends(get_conn),
):
    with conn.cursor() as cur:
        cur.execute(
            _SELECT + " WHERE fraud_probability >= %s "
            "ORDER BY fraud_probability DESC, transaction_id LIMIT %s",
            (min_probability, limit),
        )
        return cur.fetchall()
