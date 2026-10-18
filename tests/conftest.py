import csv
import tempfile
from pathlib import Path
from typing import Any, Dict, Generator, List
import pytest
from src.lakehouse.config import LakehouseConfig
from src.lakehouse.spark_session import get_spark_session, stop_spark_session


@pytest.fixture
def sample_valid_dict() -> Dict[str, Any]:
    return {
        "step": 1,
        "type": "PAYMENT",
        "amount": 9839.64,
        "nameOrig": "C1231006815",
        "oldbalanceOrg": 170136.0,
        "newbalanceOrig": 160296.36,
        "nameDest": "M1979787155",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }


@pytest.fixture
def sample_fraud_dict() -> Dict[str, Any]:
    return {
        "step": 1,
        "type": "TRANSFER",
        "amount": 181.0,
        "nameOrig": "C1305486145",
        "oldbalanceOrg": 181.0,
        "newbalanceOrig": 0.0,
        "nameDest": "C553264065",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 1,
        "isFlaggedFraud": 0,
    }


@pytest.fixture
def sample_invalid_dict() -> Dict[str, Any]:
    return {
        "step": -5,  # Invalid negative step
        "type": "UNKNOWN_ACTION",  # Invalid type
        "amount": -50.0,  # Negative amount
        "nameOrig": "",
        "oldbalanceOrg": -10.0,
        "newbalanceOrig": 0.0,
        "nameDest": "M1",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 99,
        "isFlaggedFraud": 0,
    }


@pytest.fixture
def synthetic_csv_file(tmp_path: Path) -> Path:
    """
    Creates a small synthetic CSV with 10 rows (including valid, fraud, and corrupted rows).
    """
    file_path = tmp_path / "synthetic_paysim.csv"
    headers = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg",
        "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud"
    ]

    rows: List[List[Any]] = [
        # Valid row 1: Normal Payment
        [1, "PAYMENT", 100.50, "C1000000001", 500.0, 399.50, "M2000000001", 0.0, 0.0, 0, 0],
        # Valid row 2: Normal Transfer
        [1, "TRANSFER", 250.00, "C1000000002", 1000.0, 750.0, "C3000000001", 100.0, 350.0, 0, 0],
        # Valid row 3: Fraud Transfer
        [2, "TRANSFER", 5000.00, "C1000000003", 5000.0, 0.0, "C3000000002", 0.0, 0.0, 1, 0],
        # Valid row 4: Fraud Cash Out
        [2, "CASH_OUT", 5000.00, "C1000000003", 0.0, 0.0, "M2000000002", 100.0, 5100.0, 1, 0],
        # Valid row 5: Cash In
        [3, "CASH_IN", 1500.00, "C1000000004", 200.0, 1700.0, "C3000000003", 1000.0, 0.0, 0, 0],
        # Valid row 6: Debit
        [3, "DEBIT", 50.00, "C1000000005", 800.0, 750.0, "M2000000003", 0.0, 0.0, 0, 0],
        # Invalid row 7: Negative amount
        [4, "PAYMENT", -99.00, "C1000000006", 100.0, 199.0, "M2000000004", 0.0, 0.0, 0, 0],
        # Invalid row 8: Unknown transaction type
        [4, "CRYPTO_BUY", 200.00, "C1000000007", 500.0, 300.0, "C3000000004", 0.0, 0.0, 0, 0],
        # Invalid row 9: Negative balance
        [5, "TRANSFER", 300.00, "C1000000008", -50.0, 0.0, "C3000000005", 0.0, 300.0, 0, 0],
        # Valid row 10: Normal payment
        [5, "PAYMENT", 45.20, "C1000000009", 90.0, 44.80, "M2000000005", 0.0, 0.0, 0, 0],
    ]

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return file_path


@pytest.fixture(scope="session")
def spark_session() -> Generator:
    """
    Session-scoped PySpark fixture for Lakehouse testing.
    Reuses a single local Spark context across tests for speed.
    """
    spark = get_spark_session(
        app_name="TestFraudLakehouse",
        master="local[2]",
    )
    yield spark
    stop_spark_session(spark)


@pytest.fixture
def temp_lakehouse_config(tmp_path: Path) -> LakehouseConfig:
    """
    Provides an isolated Lakehouse configuration pointing to temporary test directories.
    Ensures tests never pollute or overwrite actual workspace data.
    """
    base_dir = tmp_path / "test_lakehouse"
    base_dir.mkdir(parents=True, exist_ok=True)
    return LakehouseConfig(
        data_dir=str(base_dir),
        bronze_dir=str(base_dir / "bronze" / "transactions"),
        silver_dir=str(base_dir / "silver" / "transactions"),
        quarantine_dir=str(base_dir / "quarantine" / "transactions"),
        gold_dir=str(base_dir / "gold"),
        checkpoint_dir=str(base_dir / "checkpoints"),
        partition_by_step=False,  # Unpartitioned for fast test verification
    )


@pytest.fixture
def synthetic_duplicates_csv(tmp_path: Path) -> Path:
    """
    Creates a CSV containing 4 identical duplicate rows to verify deduplication.
    """
    file_path = tmp_path / "synthetic_duplicates.csv"
    headers = [
        "step", "type", "amount", "nameOrig", "oldbalanceOrg",
        "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
        "isFraud", "isFlaggedFraud"
    ]
    row = [1, "TRANSFER", 1000.0, "C1001", 2000.0, 1000.0, "C2001", 0.0, 1000.0, 0, 0]
    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for _ in range(4):
            writer.writerow(row)
    return file_path
