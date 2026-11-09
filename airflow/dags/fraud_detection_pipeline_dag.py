"""
Member 4: Fraud Detection Pipeline Orchestration DAG
Orchestrates: Ingestion → ETL → Quality Checks → Model Inference → Serving Load
Runs: Every 6 hours, with monitoring and alerting
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
# Repo: <root>/airflow/dags/this.py; container: /opt/airflow/dags/this.py. Use the first parent holding scripts/.
PROJECT_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "scripts").is_dir())
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
SRC_DIR = PROJECT_ROOT / "src"

# Add project to path so we can import modules
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ==================== DEFAULT ARGUMENTS ====================
default_args = {
    'owner': 'member4_data_engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@frauddetection.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# ==================== DAG DEFINITION ====================
dag = DAG(
    dag_id='fraud_detection_pipeline',
    description='Member 4: Orchestrates Members 1-3 (Ingest → ETL → ML)',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['member4', 'fraud_detection', 'production'],
)

# ==================== PYTHON FUNCTIONS ====================

def run_member1_ingestion(**context):
    """
    Member 1: Ingest transactions from CSV to Kafka
    Produces to: financial-transactions topic
    """
    logger.info("Starting Member 1: Data Ingestion (CSV → Kafka)")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "run_ingestion.py"),
        "--max-records", "100000",  # Limit for demo
        "--batch-size", "5000",
    ]

    # cwd=PROJECT_ROOT so the scripts' relative paths (data/, models/) resolve correctly
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        logger.error(f"Ingestion failed: {result.stderr}")
        raise Exception("Member 1 Ingestion failed")

    logger.info(result.stdout)
    return {"status": "success", "step": "ingestion"}


def run_member2_etl(**context):
    """
    Member 2: Run Spark ETL
    Bronze → Silver → Gold transformations
    """
    logger.info("Starting Member 2: Lakehouse ETL (Spark)")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "run_lakehouse_etl.py"),
        "--mode", "batch",
        "--format", "csv",  # input is the PaySim CSV (dataset/paysim.csv)
        "--bronze-mode", "append",
        "--silver-mode", "append",
        "--gold-mode", "overwrite",
    ]

    # cwd=PROJECT_ROOT so the scripts' relative paths (data/, models/) resolve correctly
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        logger.error(f"ETL failed: {result.stderr}")
        raise Exception("Member 2 ETL failed")

    logger.info(result.stdout)
    return {"status": "success", "step": "etl"}


def run_data_quality_checks(**context):
    """
    Member 4: Data Quality Validations
    Check: Row counts, schema, freshness, nulls
    """
    logger.info("Starting Data Quality Checks")

    from src.utils.lakehouse_telemetry import validate_bronze_layer, validate_silver_layer, validate_gold_layer

    try:
        # Validate each layer
        bronze_result = validate_bronze_layer()
        logger.info(f"Bronze Layer: {bronze_result}")

        silver_result = validate_silver_layer()
        logger.info(f"Silver Layer: {silver_result}")

        gold_result = validate_gold_layer()
        logger.info(f"Gold Layer: {gold_result}")

        # If any validation failed, raise exception
        if not all([bronze_result['passed'], silver_result['passed'], gold_result['passed']]):
            raise Exception("Data Quality checks failed")

        return {
            "status": "success",
            "bronze": bronze_result,
            "silver": silver_result,
            "gold": gold_result,
        }

    except Exception as e:
        logger.error(f"Quality check failed: {str(e)}")
        raise


def run_member3_inference(**context):
    """
    Member 3: Run Fraud Detection Model Inference
    Score transactions in Gold layer
    """
    logger.info("Starting Member 3: Fraud Detection Inference")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "predict_fraud.py"),
        "--input-path", "data/gold/enriched_transactions",
        "--output-path", "data/gold/fraud_predictions",
    ]

    # cwd=PROJECT_ROOT so the scripts' relative paths (data/, models/) resolve correctly
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        logger.error(f"Inference failed: {result.stderr}")
        raise Exception("Member 3 Inference failed")

    logger.info(result.stdout)
    return {"status": "success", "step": "inference"}


def run_member5_load(**context):
    """
    Member 5: Load fraud predictions into Postgres for the API
    """
    logger.info("Starting Member 5: Load predictions into Postgres")

    cmd = [
        sys.executable,
        str(SCRIPTS_DIR / "load_predictions_to_postgres.py"),
        "--input-path", "data/gold/fraud_predictions",
        "--run-id", context["run_id"],
    ]

    # cwd=PROJECT_ROOT so the scripts' relative paths (data/, models/) resolve correctly
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        logger.error(f"Load failed: {result.stderr}")
        raise Exception("Member 5 load failed")

    logger.info(result.stdout)
    return {"status": "success", "step": "serving_load"}


def check_pipeline_health(**context):
    """
    Final health check: Verify all layers and predictions exist
    """
    logger.info("Running final pipeline health check")

    from pathlib import Path
    import os

    required_paths = [
        "data/bronze/transactions",
        "data/silver/transactions",
        "data/gold/enriched_transactions",
        "data/gold/fraud_predictions",
    ]

    missing = []
    for path in required_paths:
        if not (PROJECT_ROOT / path).exists():  # absolute, so it works inside the container
            missing.append(path)

    if missing:
        logger.warning(f"Missing data directories: {missing}")
        raise Exception(f"Pipeline health check failed: missing {missing}")

    logger.info("✅ Pipeline health check PASSED")
    return {"status": "healthy"}


# ==================== TASKS ====================

with dag:

    # Task 1: Member 1 Ingestion
    ingest_task = PythonOperator(
        task_id='member1_ingestion',
        python_callable=run_member1_ingestion,
        doc='Ingest PaySim transactions from CSV to Kafka',
    )

    # Task 2: Member 2 ETL
    etl_task = PythonOperator(
        task_id='member2_etl',
        python_callable=run_member2_etl,
        doc='Transform data through Bronze → Silver → Gold',
    )

    # Task 3: Data Quality Checks
    quality_task = PythonOperator(
        task_id='member4_quality_checks',
        python_callable=run_data_quality_checks,
        doc='Validate data integrity at each layer',
    )

    # Task 4: Member 3 Inference
    inference_task = PythonOperator(
        task_id='member3_fraud_detection',
        python_callable=run_member3_inference,
        doc='Run ML model on enriched transactions',
    )

    # Task 5: Member 5 Serving load
    load_task = PythonOperator(
        task_id='member5_load_predictions',
        python_callable=run_member5_load,
        doc='Load fraud predictions into Postgres for the API',
    )

    # Task 6: Health Check
    health_check_task = PythonOperator(
        task_id='pipeline_health_check',
        python_callable=check_pipeline_health,
        doc='Verify all outputs exist and pipeline succeeded',
    )

    # Task 7: Success notification
    success_task = BashOperator(
        task_id='pipeline_success',
        bash_command='echo "✅ Fraud Detection Pipeline completed successfully at $(date)"',
    )

    # ==================== DEPENDENCIES ====================
    # Sequential: Ingest → ETL → Quality → Inference → Load → Health → Success
    ingest_task >> etl_task >> quality_task >> inference_task >> load_task >> health_check_task >> success_task
