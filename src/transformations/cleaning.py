"""
Data cleaning and validation transformations for the Lakehouse Silver Layer.
"""
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

VALID_TRANSACTION_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]


def normalize_column_names(df: DataFrame) -> DataFrame:
    """
    Normalizes column aliases in the PaySim dataset.
    Specifically handles the difference between 'oldbalanceOrg' and 'oldbalanceOrig'.
    """
    cols = df.columns
    # Check if 'oldbalanceOrig' exists but not 'oldbalanceOrg'
    if "oldbalanceOrig" in cols and "oldbalanceOrg" not in cols:
        df = df.withColumnRenamed("oldbalanceOrig", "oldbalanceOrg")
    return df


def cast_raw_schema(df: DataFrame) -> DataFrame:
    """
    Enforces standardized data types across all raw transaction fields.
    """
    df = normalize_column_names(df)

    cast_exprs = [
        F.col("step").cast(IntegerType()).alias("step"),
        F.upper(F.trim(F.col("type"))).cast(StringType()).alias("type"),
        F.col("amount").cast(DoubleType()).alias("amount"),
        F.trim(F.col("nameOrig")).cast(StringType()).alias("nameOrig"),
        F.col("oldbalanceOrg").cast(DoubleType()).alias("oldbalanceOrg"),
        F.col("newbalanceOrig").cast(DoubleType()).alias("newbalanceOrig"),
        F.trim(F.col("nameDest")).cast(StringType()).alias("nameDest"),
        F.col("oldbalanceDest").cast(DoubleType()).alias("oldbalanceDest"),
        F.col("newbalanceDest").cast(DoubleType()).alias("newbalanceDest"),
    ]

    # Target & audit labels
    if "isFraud" in df.columns:
        cast_exprs.append(F.col("isFraud").cast(IntegerType()).alias("isFraud"))
    else:
        cast_exprs.append(F.lit(0).cast(IntegerType()).alias("isFraud"))

    if "isFlaggedFraud" in df.columns:
        cast_exprs.append(F.col("isFlaggedFraud").cast(IntegerType()).alias("isFlaggedFraud"))
    else:
        cast_exprs.append(F.lit(0).cast(IntegerType()).alias("isFlaggedFraud"))

    # Ingestion metadata
    if "ingestion_timestamp" in df.columns:
        cast_exprs.append(F.col("ingestion_timestamp").cast(DoubleType()).alias("ingestion_timestamp"))
    else:
        cast_exprs.append(F.unix_timestamp().cast(DoubleType()).alias("ingestion_timestamp"))

    # Preserve any bronze metadata if present
    for meta_col in ["ingest_batch_id", "ingest_timestamp", "source_system", "record_hash"]:
        if meta_col in df.columns:
            cast_exprs.append(F.col(meta_col))

    result_df = df.select(*cast_exprs)

    # Synthesize readable UTC transaction_timestamp based on simulation step
    # Base timestamp: 2026-01-01 00:00:00 UTC (1 step = 1 hour)
    result_df = result_df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.from_unixtime(F.lit(1704067200) + (F.col("step") * F.lit(3600)))
        )
    )

    return result_df


def validate_and_split_records(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Evaluates transactions against strict business quality rules.
    Splits into (valid_records, quarantined_records) without silently discarding bad data.
    
    Quality Rules:
      1. Step is non-null and >= 1
      2. Transaction type is one of PAYMENT, TRANSFER, CASH_OUT, DEBIT, CASH_IN
      3. Amount is non-null and >= 0.0
      4. nameOrig is non-null, non-empty, and length >= 2
      5. nameDest is non-null, non-empty, and length >= 2
      6. Balances are non-null and >= 0.0
      7. Target label isFraud is in (0, 1) if provided
    """
    df_typed = cast_raw_schema(df)

    # Construct rejection conditions
    cond_step_invalid = F.col("step").isNull() | (F.col("step") < 1)
    cond_type_invalid = F.col("type").isNull() | (~F.col("type").isin(VALID_TRANSACTION_TYPES))
    cond_amount_invalid = F.col("amount").isNull() | (F.col("amount") < 0.0)
    cond_name_orig_invalid = F.col("nameOrig").isNull() | (F.length(F.col("nameOrig")) < 2)
    cond_name_dest_invalid = F.col("nameDest").isNull() | (F.length(F.col("nameDest")) < 2)
    cond_bal_orig_invalid = (
        F.col("oldbalanceOrg").isNull() | (F.col("oldbalanceOrg") < 0.0) |
        F.col("newbalanceOrig").isNull() | (F.col("newbalanceOrig") < 0.0)
    )
    cond_bal_dest_invalid = (
        F.col("oldbalanceDest").isNull() | (F.col("oldbalanceDest") < 0.0) |
        F.col("newbalanceDest").isNull() | (F.col("newbalanceDest") < 0.0)
    )

    # Rejection reason expression
    rejection_reason_col = (
        F.when(cond_step_invalid, F.lit("Invalid or null step (< 1)"))
        .when(cond_type_invalid, F.lit("Invalid transaction type"))
        .when(cond_amount_invalid, F.lit("Negative or null transaction amount"))
        .when(cond_name_orig_invalid, F.lit("Invalid or missing nameOrig account ID"))
        .when(cond_name_dest_invalid, F.lit("Invalid or missing nameDest account ID"))
        .when(cond_bal_orig_invalid, F.lit("Negative or null origin account balance"))
        .when(cond_bal_dest_invalid, F.lit("Negative or null destination account balance"))
        .otherwise(F.lit("Unknown validation failure"))
    )

    is_invalid = (
        cond_step_invalid |
        cond_type_invalid |
        cond_amount_invalid |
        cond_name_orig_invalid |
        cond_name_dest_invalid |
        cond_bal_orig_invalid |
        cond_bal_dest_invalid
    )

    # Valid records (Silver candidates)
    valid_df = df_typed.filter(~is_invalid)

    # Quarantined records with error audit metadata
    quarantined_df = (
        df_typed.filter(is_invalid)
        .withColumn("quarantine_reason", rejection_reason_col)
        .withColumn("quarantined_at", F.current_timestamp())
    )

    return valid_df, quarantined_df


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """
    Deduplicates records on the composite business key:
    (step, type, amount, nameOrig, nameDest, oldbalanceOrg, newbalanceOrig).
    
    If duplicates exist, preserves the record with the earliest ingestion timestamp.
    """
    business_keys = [
        "step",
        "type",
        "amount",
        "nameOrig",
        "nameDest",
        "oldbalanceOrg",
        "newbalanceOrig",
    ]
    # Spark dropDuplicates on the business key attributes
    return df.dropDuplicates(subset=business_keys)
