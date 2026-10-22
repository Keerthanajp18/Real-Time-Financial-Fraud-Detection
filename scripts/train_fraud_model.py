from pyspark.sql import SparkSession


GOLD_DATA_PATH = "data/gold/enriched_transactions"


def main():
    spark = (
        SparkSession.builder
        .appName("Member3_FraudDetection")
        .master("local[*]")
        .getOrCreate()
    )

    print("\nLoading Gold enriched transaction dataset...")

    df = spark.read.parquet(GOLD_DATA_PATH)

    print(f"\nTotal records: {df.count()}")

    print("\nDataset schema:")
    df.printSchema()

    print("\nSample transactions:")
    df.select(
        "transaction_id",
        "type",
        "amount",
        "orig_balance_error",
        "dest_balance_error",
        "is_high_risk_type",
        "isFraud"
    ).show(5, truncate=False)

    print("\nFraud label distribution:")
    df.groupBy("isFraud").count().show()

    spark.stop()


if __name__ == "__main__":
    main()