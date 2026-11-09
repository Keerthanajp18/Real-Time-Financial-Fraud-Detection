"""
Member 5: Postgres serving store for Gold fraud predictions.

Reads data/gold/fraud_predictions (Parquet, written by scripts/predict_fraud.py)
and upserts it into the `fraud` database, keyed on transaction_id.
Every load is recorded in pipeline_runs.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import parse_dsn
from psycopg2.extras import execute_values

# Same Postgres container as Airflow, separate database so serving tables stay
# apart from Airflow metadata. Override with FRAUD_DB_URL.
DEFAULT_DB_URL = "postgresql://airflow:airflow@localhost:5432/fraud"

# Data contract with Member 3 (scripts/predict_fraud.py output_columns)
PREDICTION_COLUMNS = [
    "transaction_id",
    "type",
    "amount",
    "step",
    "fraud_probability",
    "fraud_prediction",
    "model_threshold",
]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS fraud_predictions (
    transaction_id    TEXT PRIMARY KEY,
    type              TEXT NOT NULL,
    amount            DOUBLE PRECISION NOT NULL,
    step              INTEGER NOT NULL,
    fraud_probability DOUBLE PRECISION NOT NULL
                      CHECK (fraud_probability BETWEEN 0 AND 1),
    fraud_prediction  SMALLINT NOT NULL CHECK (fraud_prediction IN (0, 1)),
    model_threshold   DOUBLE PRECISION NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_predictions_probability
    ON fraud_predictions (fraud_probability DESC);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      SERIAL PRIMARY KEY,
    dag_run_id  TEXT,
    source_path TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_loaded INTEGER,
    error       TEXT,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
"""

_UPSERT_SQL = """
INSERT INTO fraud_predictions ({columns}) VALUES %s
ON CONFLICT (transaction_id) DO UPDATE SET
    {updates},
    loaded_at = now()
""".format(
    columns=", ".join(PREDICTION_COLUMNS),
    updates=",\n    ".join(f"{c} = EXCLUDED.{c}" for c in PREDICTION_COLUMNS[1:]),
)


def ensure_database(db_url: str) -> None:
    """Create the target database if it does not exist yet."""
    params = parse_dsn(db_url)
    name = params.pop("dbname")
    conn = psycopg2.connect(dbname="postgres", **params)
    conn.autocommit = True  # CREATE DATABASE cannot run inside a transaction
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    finally:
        conn.close()


def ensure_schema(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)


def load_predictions(db_url: str, source_path: str, dag_run_id=None,
                     batch_size: int = 50_000) -> int:
    """
    Upsert all predictions from the Parquet directory into fraud_predictions.

    The whole load is one transaction: on failure nothing is written to
    fraud_predictions and the pipeline_runs row is marked 'failed'.
    Returns the number of rows read from Parquet.
    """
    # Imported here so the API image does not need pyarrow
    import pyarrow.dataset as ds

    ensure_database(db_url)
    conn = psycopg2.connect(db_url)
    try:
        ensure_schema(conn)
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pipeline_runs (dag_run_id, source_path, status) "
                "VALUES (%s, %s, 'running') RETURNING run_id",
                (dag_run_id, source_path),
            )
            run_id = cur.fetchone()[0]

        try:
            rows_loaded = 0
            with conn, conn.cursor() as cur:
                # pyarrow skips Spark's _SUCCESS and .crc files
                dataset = ds.dataset(source_path, format="parquet")
                for batch in dataset.to_batches(columns=PREDICTION_COLUMNS,
                                                batch_size=batch_size):
                    rows = zip(*(batch.column(c).to_pylist() for c in PREDICTION_COLUMNS))
                    # ON CONFLICT fails if one statement hits the same key twice
                    unique_rows = list({row[0]: row for row in rows}.values())
                    execute_values(cur, _UPSERT_SQL, unique_rows, page_size=1000)
                    rows_loaded += batch.num_rows
        except Exception as exc:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE pipeline_runs SET status = 'failed', error = %s, "
                    "finished_at = now() WHERE run_id = %s",
                    (str(exc), run_id),
                )
            raise

        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE pipeline_runs SET status = 'success', rows_loaded = %s, "
                "finished_at = now() WHERE run_id = %s",
                (rows_loaded, run_id),
            )
        return rows_loaded
    finally:
        conn.close()
