"""
Member 5: Load Gold fraud predictions (Parquet) into Postgres.

Usage (from the repo root):
    python scripts/load_predictions_to_postgres.py
    python scripts/load_predictions_to_postgres.py --input-path data/gold/fraud_predictions --run-id manual
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.serving.db import DEFAULT_DB_URL, load_predictions  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description="Load fraud predictions into Postgres")
    parser.add_argument("--input-path", default="data/gold/fraud_predictions")
    parser.add_argument("--db-url", default=os.getenv("FRAUD_DB_URL", DEFAULT_DB_URL))
    parser.add_argument("--run-id", default=None, help="Airflow dag_run id, stored in pipeline_runs")
    return parser.parse_args()


def main():
    args = parse_args()
    rows = load_predictions(args.db_url, args.input_path, args.run_id)
    print(f"Loaded {rows} predictions from {args.input_path} into Postgres")


if __name__ == "__main__":
    main()
