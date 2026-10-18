import pytest
from pydantic import ValidationError
from src.models.transaction import (
    PaySimTransaction,
    TransactionType,
    validate_raw_record,
)


def test_valid_transaction_instantiation(sample_valid_dict):
    tx = PaySimTransaction(**sample_valid_dict)
    assert tx.step == 1
    assert tx.type == TransactionType.PAYMENT
    assert tx.amount == 9839.64
    assert tx.nameOrig == "C1231006815"
    assert tx.oldbalanceOrg == 170136.0
    assert tx.newbalanceOrig == 160296.36
    assert tx.isFraud == 0
    assert tx.get_partition_key() == "C1231006815"


def test_alias_normalization():
    # Test using 'oldbalanceOrig' instead of 'oldbalanceOrg'
    data = {
        "step": 2,
        "type": "TRANSFER",
        "amount": 500.0,
        "nameOrig": "C999",
        "oldbalanceOrig": 1000.0,  # alias key
        "newbalanceOrig": 500.0,
        "nameDest": "C888",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 500.0,
        "isFraud": 1,
    }
    tx = PaySimTransaction(**data)
    assert tx.oldbalanceOrg == 1000.0


def test_negative_amount_rejection(sample_valid_dict):
    invalid = sample_valid_dict.copy()
    invalid["amount"] = -10.0
    with pytest.raises(ValidationError):
        PaySimTransaction(**invalid)


def test_negative_balance_rejection(sample_valid_dict):
    invalid = sample_valid_dict.copy()
    invalid["oldbalanceOrg"] = -5.0
    with pytest.raises(ValidationError):
        PaySimTransaction(**invalid)


def test_invalid_type_rejection(sample_valid_dict):
    invalid = sample_valid_dict.copy()
    invalid["type"] = "INVALID_TYPE"
    with pytest.raises(ValidationError):
        PaySimTransaction(**invalid)


def test_invalid_fraud_flag(sample_valid_dict):
    invalid = sample_valid_dict.copy()
    invalid["isFraud"] = 2
    with pytest.raises(ValidationError):
        PaySimTransaction(**invalid)


def test_validate_raw_record_success(sample_valid_dict):
    res = validate_raw_record(sample_valid_dict, row_num=10)
    assert res.is_valid is True
    assert res.record is not None
    assert res.invalid_record is None
    assert res.record.amount == 9839.64


def test_validate_raw_record_failure(sample_invalid_dict):
    res = validate_raw_record(sample_invalid_dict, row_num=42)
    assert res.is_valid is False
    assert res.record is None
    assert res.invalid_record is not None
    assert res.invalid_record.row_number == 42
    assert len(res.invalid_record.error_message) > 0


def test_validate_raw_record_missing_field(sample_valid_dict):
    incomplete = sample_valid_dict.copy()
    del incomplete["nameDest"]
    res = validate_raw_record(incomplete, row_num=5)
    assert res.is_valid is False
    assert "Missing or empty required field: nameDest" in res.invalid_record.error_message


def test_kafka_dict_conversion(sample_valid_dict):
    tx = PaySimTransaction(**sample_valid_dict)
    d = tx.to_kafka_dict()
    assert d["type"] == "PAYMENT"
    assert "ingestion_timestamp" in d
    assert isinstance(d["amount"], float)
