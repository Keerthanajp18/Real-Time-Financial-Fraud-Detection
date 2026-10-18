from pathlib import Path
import pytest
from src.config import IngestionConfig
from src.pipeline import IngestionPipeline


def test_pipeline_synthetic_execution(synthetic_csv_file: Path):
    config = IngestionConfig(
        dataset_path=str(synthetic_csv_file),
        kafka_dry_run=True,
        batch_size=3,
        streaming_delay_seconds=0.0,
    )
    pipeline = IngestionPipeline(config=config)
    summary = pipeline.run()

    # In our synthetic dataset of 10 rows:
    # 7 rows are valid transactions
    # 3 rows are intentionally corrupted/invalid
    # 2 rows have isFraud == 1
    assert summary["total_processed"] == 10
    assert summary["valid_records"] == 7
    assert summary["invalid_records"] == 3
    assert summary["fraud_records"] == 2
    assert summary["dry_run"] is True


def test_pipeline_max_records(synthetic_csv_file: Path):
    config = IngestionConfig(
        dataset_path=str(synthetic_csv_file),
        kafka_dry_run=True,
        batch_size=2,
        max_records=4,
    )
    pipeline = IngestionPipeline(config=config)
    summary = pipeline.run()

    assert summary["total_processed"] == 4


def test_pipeline_graceful_stop(synthetic_csv_file: Path):
    config = IngestionConfig(
        dataset_path=str(synthetic_csv_file),
        kafka_dry_run=True,
        batch_size=2,
    )
    pipeline = IngestionPipeline(config=config)
    # Request stop before running
    pipeline.request_stop()
    summary = pipeline.run()

    # Because stop was requested, it should exit immediately without processing full batches
    assert summary["total_processed"] == 0
