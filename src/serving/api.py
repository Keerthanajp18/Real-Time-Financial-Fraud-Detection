"""
Member 5: REST API over the fraud predictions stored in Postgres.

Run locally:  uvicorn src.serving.api:app --reload
Docs:         http://localhost:8000/docs
"""

import os
from typing import Optional

import psycopg2
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
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
