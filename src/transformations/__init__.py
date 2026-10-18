"""
Transformations module for cleaning, schema conformance, feature engineering, and aggregations.
"""
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
from src.transformations.aggregations import (
    compute_customer_aggregates,
    compute_hourly_aggregates,
)

__all__ = [
    "cast_raw_schema",
    "deduplicate_transactions",
    "normalize_column_names",
    "validate_and_split_records",
    "build_gold_enriched_features",
    "calculate_balance_discrepancies",
    "generate_transaction_id",
    "compute_customer_aggregates",
    "compute_hourly_aggregates",
]
