import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml.functions import vector_to_array


MODEL_DIR = "models/member3"
PREPROCESSING_PATH = f"{MODEL_DIR}/safe_preprocessing_pipeline"
GBT_MODEL_PATH = f"{MODEL_DIR}/safe_gbt_model"
CONFIG_PATH = f"{MODEL_DIR}/model_config.json"


def add_safe_features(df):
    """
    Create only the features that are available at or before
    transaction scoring time.

    No post-transaction balance fields are used.
    """

    return (
        df
        .withColumn(
            "is_high_risk_type",
            F.when(
                F.col("type").isin("TRANSFER", "CASH_OUT"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "is_merchant_dest",
            F.when(
                F.col("nameDest").startswith("M"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "is_customer_dest",
            F.when(
                F.col("nameDest").startswith("C"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "orig_zero_balance_flag",
            F.when(
                F.col("oldbalanceOrg") == 0.0,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "amount_to_orig_balance_ratio",
            F.when(
                F.col("oldbalanceOrg") > 0.0,
                F.round(
                    F.col("amount") /
                    (F.col("oldbalanceOrg") + 1.0),
                    4
                )
            ).otherwise(
                F.round(F.col("amount"), 4)
            )
        )
        .withColumn(
            "is_large_transaction",
            F.when(
                F.col("amount") >= 200000.0,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Member 3 fraud detection inference"
    )

    parser.add_argument(
        "--input-path",
        default="data/gold/enriched_transactions",
        help="Input Parquet path"
    )

    parser.add_argument(
        "--output-path",
        default="data/gold/fraud_predictions",
        help="Output Parquet path"
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Member3_FraudInference")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )

    print("\nLoading model configuration...")

    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)

    threshold = float(config["threshold"])

    print(f"Model threshold: {threshold}")

    print("\nLoading input transactions...")

    df = spark.read.parquet(args.input_path)

    print(f"Input rows: {df.count():,}")

    print("\nCreating deployment-safe features...")

    feature_df = add_safe_features(df)

    print("\nLoading preprocessing pipeline...")

    preprocessing_model = PipelineModel.load(
        PREPROCESSING_PATH
    )

    prepared_df = preprocessing_model.transform(
        feature_df
    )

    print("\nLoading trained GBT fraud model...")

    gbt_model = GBTClassificationModel.load(
        GBT_MODEL_PATH
    )

    predictions = gbt_model.transform(
        prepared_df
    )

    predictions = (
        predictions
        .withColumn(
            "fraud_probability",
            vector_to_array("probability")[1]
        )
        .withColumn(
            "fraud_prediction",
            F.when(
                F.col("fraud_probability") >= threshold,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "model_threshold",
            F.lit(threshold)
        )
    )

    output_columns = [
        "transaction_id",
        "type",
        "amount",
        "step",
        "fraud_probability",
        "fraud_prediction",
        "model_threshold",
    ]

    output_df = predictions.select(
        *output_columns
    )

    print("\nPrediction distribution:")

    output_df.groupBy(
        "fraud_prediction"
    ).count().orderBy(
        "fraud_prediction"
    ).show()

    print("\nExample fraud predictions:")

    output_df.filter(
        F.col("fraud_prediction") == 1
    ).orderBy(
        F.col("fraud_probability").desc()
    ).show(
        10,
        truncate=False
    )

    print(
        f"\nWriting predictions to: "
        f"{args.output_path}"
    )

    (
        output_df.write
        .mode("overwrite")
        .parquet(args.output_path)
    )

    print("\nFraud inference completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()