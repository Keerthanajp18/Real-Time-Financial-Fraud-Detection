from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

GOLD_DATA_PATH = "data/gold/enriched_transactions"

LABEL_COLUMN = "isFraud"

NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
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
    "step",
]

CATEGORICAL_FEATURES = [
    "type",
]


def main():
    spark = (
        SparkSession.builder
        .appName("Member3_FraudDetection")
        .master("local[*]")
        .getOrCreate()
    )

    print("\nLoading Gold enriched transaction dataset...")
    df = spark.read.parquet(GOLD_DATA_PATH)

    type_indexer = StringIndexer(
        inputCol="type",
        outputCol="type_index",
        handleInvalid="keep"
    )

    type_encoder = OneHotEncoder(
        inputCols=["type_index"],
        outputCols=["type_encoded"]
    )

    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + ["type_encoded"],
        outputCol="features"
    )

    pipeline = Pipeline(
        stages=[
            type_indexer,
            type_encoder,
            assembler
        ]
    )

    pipeline_model = pipeline.fit(df)
    ml_df = pipeline_model.transform(df)

    print("\nML-ready dataset:")
    ml_df.select(
        "transaction_id",
        "type",
        "type_index",
        "type_encoded",
        "features",
        "isFraud"
    ).show(5, truncate=False)

    print(f"\nTotal records: {df.count()}")

    print("\nTarget column:")
    print(LABEL_COLUMN)

    print("\nNumeric features:")
    for feature in NUMERIC_FEATURES:
        print(f" - {feature}")

    print("\nCategorical features:")
    for feature in CATEGORICAL_FEATURES:
        print(f" - {feature}")

    print("\nFraud label distribution:")
    df.groupBy(LABEL_COLUMN).count().show()

    spark.stop()


if __name__ == "__main__":
    main()