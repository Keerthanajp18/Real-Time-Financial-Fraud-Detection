import time
from enum import Enum
from typing import Any, Dict, Optional, Tuple
from pydantic import BaseModel, Field, field_validator, model_validator


class TransactionType(str, Enum):
    PAYMENT = "PAYMENT"
    TRANSFER = "TRANSFER"
    CASH_OUT = "CASH_OUT"
    DEBIT = "DEBIT"
    CASH_IN = "CASH_IN"


class PaySimTransaction(BaseModel):
    """
    Validated PaySim financial transaction record.
    Supports both 'oldbalanceOrg' (dataset header spelling) and 'oldbalanceOrig'.
    """
    step: int = Field(..., ge=1, description="Unit of time in simulation (1 step = 1 hour)")
    type: TransactionType = Field(..., description="Transaction type")
    amount: float = Field(..., ge=0.0, description="Transaction amount")
    nameOrig: str = Field(..., min_length=2, description="Originating customer ID")
    oldbalanceOrg: float = Field(..., ge=0.0, description="Initial balance of origin account")
    newbalanceOrig: float = Field(..., ge=0.0, description="New balance of origin account")
    nameDest: str = Field(..., min_length=2, description="Recipient account ID")
    oldbalanceDest: float = Field(..., ge=0.0, description="Initial balance of recipient account")
    newbalanceDest: float = Field(..., ge=0.0, description="New balance of recipient account")
    isFraud: int = Field(..., ge=0, le=1, description="Ground truth target label (1=Fraud, 0=Normal). Not for live feature input.")
    isFlaggedFraud: int = Field(0, ge=0, le=1, description="Audit/business rule flag (1=Flagged, 0=Normal). Not for live feature input.")
    ingestion_timestamp: float = Field(default_factory=time.time, description="Unix timestamp of ingestion")

    @model_validator(mode="before")
    @classmethod
    def normalize_aliases_and_types(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Normalise oldbalanceOrg / oldbalanceOrig
            if "oldbalanceOrg" not in data and "oldbalanceOrig" in data:
                data["oldbalanceOrg"] = data["oldbalanceOrig"]
            elif "oldbalanceOrig" not in data and "oldbalanceOrg" in data:
                data["oldbalanceOrig"] = data["oldbalanceOrg"]
            
            # Normalise string strip
            for key in ["nameOrig", "nameDest", "type"]:
                if key in data and isinstance(data[key], str):
                    data[key] = data[key].strip()
        return data

    @field_validator("nameOrig")
    @classmethod
    def validate_name_orig(cls, v: str) -> str:
        if not v or len(v) < 2:
            raise ValueError(f"Invalid origin account ID: '{v}'")
        return v

    @field_validator("nameDest")
    @classmethod
    def validate_name_dest(cls, v: str) -> str:
        if not v or len(v) < 2:
            raise ValueError(f"Invalid destination account ID: '{v}'")
        return v

    def get_partition_key(self) -> str:
        """
        Returns origin account identifier to use as Kafka message key.
        This guarantees strict in-order processing of transactions per customer.
        """
        return self.nameOrig

    def to_kafka_dict(self) -> Dict[str, Any]:
        """Convert transaction to serializable dictionary for Kafka message payload."""
        data = self.model_dump()
        data["type"] = self.type.value
        return data


class InvalidTransactionRecord(BaseModel):
    """
    Schema for quarantine/dead-letter-queue transactions failing validation.
    """
    raw_data: Dict[str, Any]
    error_message: str
    row_number: Optional[int] = None
    ingestion_timestamp: float = Field(default_factory=time.time)


class ValidationResult(BaseModel):
    """
    Encapsulates outcome of validating a raw row.
    """
    is_valid: bool
    record: Optional[PaySimTransaction] = None
    invalid_record: Optional[InvalidTransactionRecord] = None


def validate_raw_record(raw: Dict[str, Any], row_num: Optional[int] = None) -> ValidationResult:
    """
    Validates a raw dictionary against PaySim schema.
    Returns ValidationResult containing either validated PaySimTransaction or InvalidTransactionRecord.
    """
    try:
        # Check required fields exist
        required_keys = [
            "step", "type", "amount", "nameOrig",
            "newbalanceOrig", "nameDest", "oldbalanceDest",
            "newbalanceDest", "isFraud"
        ]
        has_old_bal = "oldbalanceOrg" in raw or "oldbalanceOrig" in raw
        if not has_old_bal:
            missing = "oldbalanceOrg/oldbalanceOrig"
            return ValidationResult(
                is_valid=False,
                invalid_record=InvalidTransactionRecord(
                    raw_data=raw,
                    error_message=f"Missing required field: {missing}",
                    row_number=row_num,
                ),
            )

        for req in required_keys:
            if req not in raw or raw[req] is None or raw[req] == "":
                return ValidationResult(
                    is_valid=False,
                    invalid_record=InvalidTransactionRecord(
                        raw_data=raw,
                        error_message=f"Missing or empty required field: {req}",
                        row_number=row_num,
                    ),
                )

        validated = PaySimTransaction(**raw)
        return ValidationResult(is_valid=True, record=validated)

    except Exception as exc:
        return ValidationResult(
            is_valid=False,
            invalid_record=InvalidTransactionRecord(
                raw_data=raw,
                error_message=str(exc),
                row_number=row_num,
            ),
        )
