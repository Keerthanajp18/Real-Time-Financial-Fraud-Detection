"""
Verification script for Member 2 Lakehouse Parquet outputs.
Inspects Bronze, Silver, Quarantine, and Gold datasets without modifying them.
"""
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.lakehouse.spark_session import get_spark_session, stop_spark_session

EXPECTED_MEMBER3_COLUMNS = [
    "transaction_id",
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "transaction_timestamp",
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
    "isFraud",
    "isFlaggedFraud",
]


def main():
    spark = get_spark_session(app_name="VerifyLakehouseOutputs", master="local[1]")

    base_data = PROJECT_ROOT / "data"
    datasets = {
        "Bronze (Raw Transactions)": base_data / "bronze" / "transactions",
        "Silver (Cleaned Transactions)": base_data / "silver" / "transactions",
        "Quarantine (Invalid Transactions)": base_data / "quarantine" / "transactions",
        "Gold Enriched Transactions (ML Features)": base_data / "gold" / "enriched_transactions",
        "Gold Customer Aggregates": base_data / "gold" / "customer_aggregates",
        "Gold Hourly Aggregates": base_data / "gold" / "hourly_aggregates",
    }

    print("=" * 80)
    print("MEMBER 2 LAKEHOUSE PARQUET VERIFICATION REPORT")
    print("=" * 80)

    loaded_dfs = {}

    # 1 & 2. Read each dataset, print row counts and column names
    print("\n--- 1. DATASET ROW COUNTS & SCHEMAS ---")
    for name, path in datasets.items():
        if not path.exists():
            print(f"\n[MISSING] {name} at {path}")
            continue

        df = spark.read.parquet(str(path))
        loaded_dfs[name] = df
        row_count = df.count()
        columns = df.columns

        print(f"\n* {name}")
        print(f"  Path:       {path.relative_to(PROJECT_ROOT)}")
        print(f"  Row Count:  {row_count}")
        print(f"  Columns ({len(columns)}): {columns}")

    # 3. Check isFraud in Gold Enriched Transactions
    print("\n--- 2. GROUND TRUTH LABELS (isFraud / isFlaggedFraud) IN GOLD ENRICHED ---")
    gold_enriched = loaded_dfs.get("Gold Enriched Transactions (ML Features)")
    if gold_enriched is not None:
        has_fraud = "isFraud" in gold_enriched.columns
        has_flagged = "isFlaggedFraud" in gold_enriched.columns
        print(f"  'isFraud' present in Gold Enriched: {has_fraud}")
        print(f"  'isFlaggedFraud' present in Gold Enriched: {has_flagged}")

        if has_fraud:
            print("\n  'isFraud' value distribution in Gold Enriched:")
            gold_enriched.groupBy("isFraud").count().show()

            print("  Fraud transactions sample:")
            gold_enriched.filter("isFraud = 1").select(
                "transaction_id", "type", "amount", "nameOrig", "nameDest", "orig_balance_error", "isFraud"
            ).show(truncate=False)

    # 4. Verify all feature columns listed in docs/MEMBER2_DATA_ENGINEERING.md
    print("\n--- 3. VERIFICATION OF MEMBER 3 FEATURE COLUMNS ---")
    if gold_enriched is not None:
        actual_cols = set(gold_enriched.columns)
        missing_cols = [c for c in EXPECTED_MEMBER3_COLUMNS if c not in actual_cols]
        extra_cols = [c for c in actual_cols if c not in EXPECTED_MEMBER3_COLUMNS]

        print(f"  Total Expected Columns from Documentation: {len(EXPECTED_MEMBER3_COLUMNS)}")
        print(f"  Total Actual Columns in Parquet:          {len(actual_cols)}")

        if not missing_cols:
            print("  [PASSED] All 23 feature and metadata columns match documentation exactly!")
        else:
            print(f"  [FAILED] Missing columns: {missing_cols}")

        if extra_cols:
            print(f"  Additional columns present (metadata/audit): {extra_cols}")

        print("\n  Sample Features from Gold Enriched (first 3 rows):")
        gold_enriched.select(
            "transaction_id", "type", "amount", "orig_balance_error", "dest_balance_error",
            "is_merchant_dest", "dest_zero_balance_flag", "amount_to_orig_balance_ratio", "isFraud"
        ).show(3, truncate=False)

    # 5. Quarantine Inspection & Reasons
    print("\n--- 4. QUARANTINE INSPECTION & REJECTION REASONS ---")
    quarantine_df = loaded_dfs.get("Quarantine (Invalid Transactions)")
    if quarantine_df is not None:
        q_count = quarantine_df.count()
        print(f"  Total Quarantined Records: {q_count}")
        print("\n  Quarantined Records Details:")
        quarantine_df.select(
            "step", "type", "amount", "nameOrig", "nameDest", "quarantine_reason", "quarantined_at"
        ).show(truncate=False)

    # 6. Customer & Hourly Aggregates inspection
    print("\n--- 5. AGGREGATES VALIDATION ---")
    cust_agg = loaded_dfs.get("Gold Customer Aggregates")
    if cust_agg is not None:
        print(f"  Customer Profiles Count: {cust_agg.count()}")
        print("  Sample Customer Profile:")
        cust_agg.show(2, truncate=False)

    hourly_agg = loaded_dfs.get("Gold Hourly Aggregates")
    if hourly_agg is not None:
        print(f"  Hourly Rollups Count: {hourly_agg.count()}")
        print("  Hourly Rollup Details:")
        hourly_agg.show(truncate=False)

    print("=" * 80)
    print("VERIFICATION COMPLETE")
    print("=" * 80)

    stop_spark_session(spark)


if __name__ == "__main__":
    main()
