"""
Feature engineering transformations for the Lakehouse Gold Layer.
Prepares ML-ready and analytics-ready features for downstream consumers (Member 3: Fraud Detection & ML).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


def generate_transaction_id(df: DataFrame) -> DataFrame:
    """
    Synthesizes a deterministic unique identifier for each transaction
    based on SHA-256 hash of its composite immutable fields.
    """
    id_expr = F.sha2(
        F.concat_ws(
            "||",
            F.col("step").cast(StringType()),
            F.col("type"),
            F.col("amount").cast(StringType()),
            F.col("nameOrig"),
            F.col("nameDest"),
            F.col("oldbalanceOrg").cast(StringType()),
            F.col("newbalanceOrig").cast(StringType()),
        ),
        256,
    )
    return df.withColumn("transaction_id", id_expr)


def calculate_balance_discrepancies(df: DataFrame) -> DataFrame:
    """
    Calculates accounting balance discrepancy features:
      - orig_balance_error: (newbalanceOrig + amount - oldbalanceOrg)
        In legitimate debit/transfer transactions, this should be ~0. Deviations strongly correlate with fraud.
      - dest_balance_error: (oldbalanceDest + amount - newbalanceDest)
        In transfers to normal accounts, destination balance should increase by amount.
    """
    return (
        df.withColumn(
            "orig_balance_error",
            F.round(F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg"), 2)
        )
        .withColumn(
            "dest_balance_error",
            F.round(F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest"), 2)
        )
    )


def build_gold_enriched_features(df: DataFrame) -> DataFrame:
    """
    Transforms Silver cleaned transactions into Gold enriched features.
    
    CRITICAL: Preserves ground-truth 'isFraud' and 'isFlaggedFraud' strictly as
    evaluation targets. Live predictive features are derived ONLY from transaction
    attributes, entity accounts, amounts, and mathematical balance relationships.
    """
    # 1. Attach deterministic transaction identifier
    df_with_id = generate_transaction_id(df)

    # 2. Add balance discrepancies
    df_with_errors = calculate_balance_discrepancies(df_with_id)

    # 3. Add domain-specific behavioral flags & ratios
    enriched_df = (
        df_with_errors
        # High risk transaction types (PaySim fraud only occurs in TRANSFER and CASH_OUT)
        .withColumn(
            "is_high_risk_type",
            F.when(F.col("type").isin("TRANSFER", "CASH_OUT"), F.lit(1)).otherwise(F.lit(0))
        )
        # Destination entity type
        .withColumn(
            "is_merchant_dest",
            F.when(F.col("nameDest").startswith("M"), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "is_customer_dest",
            F.when(F.col("nameDest").startswith("C"), F.lit(1)).otherwise(F.lit(0))
        )
        # Empty balance anomalies
        .withColumn(
            "orig_zero_balance_flag",
            F.when(F.col("oldbalanceOrg") == 0.0, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "dest_zero_balance_flag",
            F.when(
                (F.col("oldbalanceDest") == 0.0) & (F.col("newbalanceDest") == 0.0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        # Full balance transfer indicator (entire origin balance emptied)
        .withColumn(
            "is_orig_fully_emptied",
            F.when(
                (F.col("oldbalanceOrg") > 0.0) & (F.col("newbalanceOrig") == 0.0),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        # Ratio of amount to original origin balance
        .withColumn(
            "amount_to_orig_balance_ratio",
            F.when(
                F.col("oldbalanceOrg") > 0.0,
                F.round(F.col("amount") / (F.col("oldbalanceOrg") + 1.0), 4)
            ).otherwise(F.round(F.col("amount"), 4))
        )
        # Large transaction flag (above business audit threshold of 200,000)
        .withColumn(
            "is_large_transaction",
            F.when(F.col("amount") >= 200000.0, F.lit(1)).otherwise(F.lit(0))
        )
    )

    return enriched_df
