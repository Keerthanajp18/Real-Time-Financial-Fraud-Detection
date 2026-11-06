"""
Unit & Integration Tests for Lakehouse Layers and Transformations (Member 2).
Covers Bronze ingestion metadata, Silver quality validation, quarantine routing,
deduplication, feature engineering, customer aggregations, and Parquet persistence.
"""
from pathlib import Path
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from src.lakehouse.bronze import BronzeLayerManager
from src.lakehouse.config import LakehouseConfig
from src.lakehouse.gold import GoldLayerManager
from src.lakehouse.silver import SilverLayerManager
from src.transformations.aggregations import (
    compute_customer_aggregates,
    compute_hourly_aggregates,
)
from src.transformations.cleaning import (
    cast_raw_schema,
    deduplicate_transactions,
    normalize_column_names,
    validate_and_split_records,
)
from src.transformations.features import (
    build_gold_enriched_features,
    calculate_balance_discrepancies,
    generate_transaction_id,
)


def test_normalize_column_names(spark_session: SparkSession):
    """Verifies that 'oldbalanceOrig' alias is normalized to 'oldbalanceOrg'."""
    data = [(1, "TRANSFER", 100.0, 500.0)]
    df = spark_session.createDataFrame(data, ["step", "type", "amount", "oldbalanceOrig"])
    normalized = normalize_column_names(df)
    assert "oldbalanceOrg" in normalized.columns
    assert "oldbalanceOrig" not in normalized.columns


def test_cast_raw_schema(spark_session: SparkSession, synthetic_csv_file: Path):
    """Verifies strict schema casting and timestamp synthesis."""
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    typed_df = cast_raw_schema(raw_df)

    schema_dict = dict(typed_df.dtypes)
    assert schema_dict["step"] == "int"
    assert schema_dict["type"] == "string"
    assert schema_dict["amount"] == "double"
    assert schema_dict["nameOrig"] == "string"
    assert schema_dict["oldbalanceOrg"] == "double"
    assert schema_dict["newbalanceOrig"] == "double"
    assert schema_dict["nameDest"] == "string"
    assert schema_dict["oldbalanceDest"] == "double"
    assert schema_dict["newbalanceDest"] == "double"
    assert schema_dict["isFraud"] == "int"
    assert schema_dict["isFlaggedFraud"] == "int"
    assert schema_dict["ingestion_timestamp"] == "double"
    assert schema_dict["transaction_timestamp"] == "timestamp"


def test_validate_and_split_records(spark_session: SparkSession, synthetic_csv_file: Path):
    """
    Tests that synthetic dataset (10 rows) is correctly partitioned into
    7 valid records and 3 quarantined records (negative amount, unknown type, negative balance).
    """
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    valid_df, quarantined_df = validate_and_split_records(raw_df)

    valid_count = valid_df.count()
    quarantined_count = quarantined_df.count()

    assert valid_count == 7
    assert quarantined_count == 3

    # Verify quarantine columns
    assert "quarantine_reason" in quarantined_df.columns
    assert "quarantined_at" in quarantined_df.columns

    # Verify reasons
    reasons = [row["quarantine_reason"] for row in quarantined_df.collect()]
    assert any("Negative or null transaction amount" in r for r in reasons)
    assert any("Invalid transaction type" in r for r in reasons)
    assert any("Negative or null origin account balance" in r for r in reasons)


def test_deduplicate_transactions(spark_session: SparkSession, synthetic_duplicates_csv: Path):
    """Tests deduplication on composite business key."""
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_duplicates_csv))
    typed_df = cast_raw_schema(raw_df)

    assert typed_df.count() == 4
    dedup_df = deduplicate_transactions(typed_df)
    assert dedup_df.count() == 1


def test_generate_transaction_id(spark_session: SparkSession, synthetic_csv_file: Path):
    """Verifies that deterministic SHA-256 transaction_id is generated."""
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    typed_df = cast_raw_schema(raw_df)
    with_id_df = generate_transaction_id(typed_df)

    assert "transaction_id" in with_id_df.columns
    first_id = with_id_df.select("transaction_id").first()["transaction_id"]
    assert isinstance(first_id, str)
    assert len(first_id) == 64  # SHA-256 hex string length


def test_balance_discrepancy_and_gold_features(spark_session: SparkSession, synthetic_csv_file: Path):
    """Tests balance error calculations and engineered Gold features."""
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    valid_df, _ = validate_and_split_records(raw_df)
    gold_df = build_gold_enriched_features(valid_df)

    expected_cols = [
        "transaction_id",
        "orig_balance_error",
        "dest_balance_error",
        "is_high_risk_type",
        "is_merchant_dest",
        "is_customer_dest",
        "orig_zero_balance_flag",
        "dest_zero_balance_flag",
        "is_orig_fully_emptied",
        "amount_to_orig_balance_ratio",
        "is_large_transaction",
        "isFraud",  # Preserved as target
    ]
    for col_name in expected_cols:
        assert col_name in gold_df.columns, f"Missing feature column: {col_name}"

    # Verify merchant flag
    first_row = gold_df.filter("type = 'PAYMENT'").first()
    assert first_row["is_merchant_dest"] == 1
    assert first_row["is_customer_dest"] == 0


def test_customer_and_hourly_aggregates(spark_session: SparkSession, synthetic_csv_file: Path):
    """Tests Gold customer-level and hour-level aggregations."""
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    valid_df, _ = validate_and_split_records(raw_df)
    gold_df = build_gold_enriched_features(valid_df)

    customer_agg = compute_customer_aggregates(gold_df)
    assert "customer_id" in customer_agg.columns
    assert "total_transactions" in customer_agg.columns
    assert "total_amount" in customer_agg.columns
    assert customer_agg.count() > 0

    hourly_agg = compute_hourly_aggregates(gold_df)
    assert "step" in hourly_agg.columns
    assert "hourly_transaction_count" in hourly_agg.columns
    assert "hourly_total_amount" in hourly_agg.columns
    assert hourly_agg.count() > 0


def test_bronze_layer_write_and_read(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """Tests writing raw data to Bronze Parquet and reading back with audit metadata."""
    manager = BronzeLayerManager(config=temp_lakehouse_config)
    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))

    metrics = manager.write_bronze(raw_df, source_name="synthetic_csv", mode="overwrite")
    assert metrics["records_written"] == 10

    read_back = manager.read_bronze(spark_session)
    assert read_back.count() == 10
    assert "ingest_batch_id" in read_back.columns
    assert "ingest_timestamp" in read_back.columns
    assert "record_hash" in read_back.columns


def test_silver_layer_process_and_quarantine(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """Tests Bronze to Silver processing, quarantine partitioning, and deduplication."""
    bronze_mgr = BronzeLayerManager(config=temp_lakehouse_config)
    silver_mgr = SilverLayerManager(config=temp_lakehouse_config)

    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    bronze_mgr.write_bronze(raw_df, source_name="test_raw", mode="overwrite")
    bronze_df = bronze_mgr.read_bronze(spark_session)

    silver_metrics = silver_mgr.process_and_write(bronze_df, mode="overwrite")
    assert silver_metrics["silver_records"] == 7
    assert silver_metrics["quarantined_records"] == 3

    # Verify persisted Parquet data
    silver_read = silver_mgr.read_silver(spark_session)
    quarantine_read = silver_mgr.read_quarantine(spark_session)

    assert silver_read.count() == 7
    assert quarantine_read.count() == 3


def test_gold_layer_process_and_read(
    spark_session: SparkSession,
    synthetic_csv_file: Path,
    temp_lakehouse_config: LakehouseConfig,
):
    """Tests Silver to Gold processing and verifies all three Gold output tables."""
    bronze_mgr = BronzeLayerManager(config=temp_lakehouse_config)
    silver_mgr = SilverLayerManager(config=temp_lakehouse_config)
    gold_mgr = GoldLayerManager(config=temp_lakehouse_config)

    raw_df = spark_session.read.option("header", "true").csv(str(synthetic_csv_file))
    bronze_mgr.write_bronze(raw_df, source_name="test_raw", mode="overwrite")
    bronze_df = bronze_mgr.read_bronze(spark_session)
    silver_mgr.process_and_write(bronze_df, mode="overwrite")

    silver_df = silver_mgr.read_silver(spark_session)
    gold_metrics = gold_mgr.process_and_write(silver_df, mode="overwrite")

    assert gold_metrics["enriched_transactions_count"] == 7
    assert gold_metrics["customer_aggregates_count"] > 0
    assert gold_metrics["hourly_aggregates_count"] > 0

    # Verify reading tables back
    enriched_read = gold_mgr.read_enriched_transactions(spark_session)
    customer_read = gold_mgr.read_customer_aggregates(spark_session)
    hourly_read = gold_mgr.read_hourly_aggregates(spark_session)

    assert enriched_read.count() == 7
    assert customer_read.count() > 0
    assert hourly_read.count() > 0
