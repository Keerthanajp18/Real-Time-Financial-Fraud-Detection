"""
Aggregation transformations for Gold Layer customer profiles and time-based metrics.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_customer_aggregates(df: DataFrame) -> DataFrame:
    """
    Computes account-level behavioral profiles for originating customers (nameOrig).
    Helps downstream ML detect anomalous spending velocity and deviation from user baseline.
    """
    agg_df = df.groupBy("nameOrig").agg(
        F.count("*").alias("total_transactions"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.max("amount"), 2).alias("max_amount"),
        F.round(F.min("amount"), 2).alias("min_amount"),
        F.sum(F.when(F.col("type") == "TRANSFER", 1).otherwise(0)).alias("transfer_count"),
        F.sum(F.when(F.col("type") == "CASH_OUT", 1).otherwise(0)).alias("cash_out_count"),
        F.sum(F.when(F.col("type") == "PAYMENT", 1).otherwise(0)).alias("payment_count"),
        F.sum(F.when(F.col("type") == "CASH_IN", 1).otherwise(0)).alias("cash_in_count"),
        F.sum(F.when(F.col("type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
        F.sum(F.when(F.col("oldbalanceOrg") == 0.0, 1).otherwise(0)).alias("zero_bal_orig_count"),
        F.round(
            F.avg(
                F.when(
                    F.col("orig_balance_error").isNotNull(),
                    F.col("orig_balance_error")
                ).otherwise(0.0)
            ),
            2
        ).alias("avg_orig_balance_error"),
    )
    return agg_df.withColumnRenamed("nameOrig", "customer_id")


def compute_hourly_aggregates(df: DataFrame) -> DataFrame:
    """
    Computes temporal summary metrics grouped by simulation step (hour).
    Useful for platform monitoring, traffic surge analysis, and fraud pattern detection.
    """
    hourly_df = df.groupBy("step").agg(
        F.count("*").alias("hourly_transaction_count"),
        F.round(F.sum("amount"), 2).alias("hourly_total_amount"),
        F.round(F.avg("amount"), 2).alias("hourly_avg_amount"),
        F.round(F.max("amount"), 2).alias("hourly_max_amount"),
        F.sum(F.when(F.col("type") == "TRANSFER", 1).otherwise(0)).alias("hourly_transfer_count"),
        F.sum(F.when(F.col("type") == "CASH_OUT", 1).otherwise(0)).alias("hourly_cash_out_count"),
        F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("hourly_fraud_count"),
        F.round(
            F.sum(F.when(F.col("isFraud") == 1, F.col("amount")).otherwise(0.0)),
            2
        ).alias("hourly_fraud_amount"),
    ).orderBy("step")

    return hourly_df
