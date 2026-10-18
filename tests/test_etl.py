"""
Integration and End-to-End Tests for Batch Lakehouse ETL Pipeline (Member 2).
Covers complete pipeline execution, repeated/idempotent runs, max_records, empty files,
and multi-format inputs (CSV, JSON).
"""
import json
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from src.etl.batch_etl import BatchLakehouseETL
from src.lakehouse.config import LakehouseConfig


def test_batch_etl_end_to_end(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """
    Tests full End-to-End Batch Lakehouse ETL pipeline execution on synthetic data:
    Extract -> Bronze -> Silver -> Gold.
    """
    etl = BatchLakehouseETL(spark=spark_session, config=temp_lakehouse_config)
    summary = etl.run(
        input_path=str(synthetic_csv_file),
        file_format="csv",
        bronze_mode="overwrite",
        silver_mode="overwrite",
        gold_mode="overwrite",
    )

    stages = summary["stages"]
    assert stages["extract"]["records_read"] == 10
    assert stages["bronze"]["records_written"] == 10
    assert stages["silver"]["silver_records"] == 7
    assert stages["silver"]["quarantined_records"] == 3
    assert stages["gold"]["enriched_transactions_count"] == 7
    assert stages["gold"]["customer_aggregates_count"] > 0
    assert stages["gold"]["hourly_aggregates_count"] > 0


def test_batch_etl_empty_dataset(
    spark_session: SparkSession,
    tmp_path: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """Verifies that an empty dataset halts gracefully without crashing."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n", encoding="utf-8")

    etl = BatchLakehouseETL(spark=spark_session, config=temp_lakehouse_config)
    summary = etl.run(input_path=str(empty_csv), file_format="csv")

    assert summary["stages"]["extract"]["records_read"] == 0
    assert "bronze" not in summary["stages"]


def test_batch_etl_max_records(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """Tests capping batch processing with max_records parameter."""
    etl = BatchLakehouseETL(spark=spark_session, config=temp_lakehouse_config)
    summary = etl.run(
        input_path=str(synthetic_csv_file),
        file_format="csv",
        max_records=5,
        bronze_mode="overwrite",
        silver_mode="overwrite",
        gold_mode="overwrite",
    )

    assert summary["stages"]["extract"]["records_read"] == 5
    assert summary["stages"]["bronze"]["records_written"] == 5


def test_batch_etl_unsupported_format(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """Verifies that attempting an unsupported format raises ValueError."""
    etl = BatchLakehouseETL(spark=spark_session, config=temp_lakehouse_config)
    with pytest.raises(ValueError, match="Unsupported file format"):
        etl.extract(input_path=str(synthetic_csv_file), file_format="avro")


def test_repeated_pipeline_execution(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """
    Tests pipeline idempotency and safe re-execution.
    Running the pipeline twice with overwrite mode should produce consistent results.
    """
    etl = BatchLakehouseETL(spark=spark_session, config=temp_lakehouse_config)

    # First run
    summary_1 = etl.run(
        input_path=str(synthetic_csv_file),
        file_format="csv",
        bronze_mode="overwrite",
        silver_mode="overwrite",
        gold_mode="overwrite",
    )

    # Second run
    summary_2 = etl.run(
        input_path=str(synthetic_csv_file),
        file_format="csv",
        bronze_mode="overwrite",
        silver_mode="overwrite",
        gold_mode="overwrite",
    )

    assert summary_1["stages"]["silver"]["silver_records"] == summary_2["stages"]["silver"]["silver_records"]
    assert summary_1["stages"]["gold"]["enriched_transactions_count"] == summary_2["stages"]["gold"]["enriched_transactions_count"]


def test_batch_etl_json_source(
    spark_session: SparkSession,
    tmp_path: Path,
    temp_lakehouse_config: LakehouseConfig,
    sample_valid_dict: dict,
    sample_fraud_dict: dict,
):
    """Verifies that BatchLakehouseETL can read from JSON input source (e.g., Kafka extract dump)."""
    json_file = tmp_path / "transactions.json"
    with open(json_file, mode="w", encoding="utf-8") as f:
        f.write(json.dumps(sample_valid_dict) + "\n")
        f.write(json.dumps(sample_fraud_dict) + "\n")

    etl = BatchLakehouseETL(spark=spark_session, config=temp_lakehouse_config)
    summary = etl.run(
        input_path=str(json_file),
        file_format="json",
        bronze_mode="overwrite",
        silver_mode="overwrite",
        gold_mode="overwrite",
    )

    assert summary["stages"]["extract"]["records_read"] == 2
    assert summary["stages"]["bronze"]["records_written"] == 2
    assert summary["stages"]["silver"]["silver_records"] == 2
    assert summary["stages"]["gold"]["enriched_transactions_count"] == 2
