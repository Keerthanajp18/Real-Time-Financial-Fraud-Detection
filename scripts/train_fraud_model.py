from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)

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

    train_raw_df, test_raw_df = df.randomSplit(
    [0.8, 0.2],
    seed=42
    )

    pipeline_model = pipeline.fit(train_raw_df)

    train_df = pipeline_model.transform(train_raw_df)
    test_df = pipeline_model.transform(test_raw_df)

    print("\nTraining Logistic Regression baseline...")

    lr = LogisticRegression(
        featuresCol="features",
        labelCol=LABEL_COLUMN,
        maxIter=20
    )

    lr_model = lr.fit(train_df)

    predictions = lr_model.transform(test_df)

    confusion_rows = (
        predictions
        .groupBy(LABEL_COLUMN, "prediction")
        .count()
        .collect()
    )

    confusion = {
        (int(row[LABEL_COLUMN]), int(row["prediction"])): row["count"]
        for row in confusion_rows
    }

    tn = confusion.get((0, 0), 0)
    fp = confusion.get((0, 1), 0)
    fn = confusion.get((1, 0), 0)
    tp = confusion.get((1, 1), 0)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    fraud_f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0.0
    )

    accuracy = (tp + tn) / (tp + tn + fp + fn)

    pr_evaluator = BinaryClassificationEvaluator(
    labelCol=LABEL_COLUMN,
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR"
    )

    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol=LABEL_COLUMN,
        predictionCol="prediction",
        metricName="f1"
    )

    pr_auc = pr_evaluator.evaluate(predictions)
    roc_auc = roc_evaluator.evaluate(predictions)
    f1_score = f1_evaluator.evaluate(predictions)

    print("\nLogistic Regression Evaluation:")
    print(f"PR-AUC:          {pr_auc:.4f}")
    print(f"ROC-AUC:         {roc_auc:.4f}")
    print(f"Fraud Precision: {precision:.4f}")
    print(f"Fraud Recall:    {recall:.4f}")
    print(f"Fraud F1:        {fraud_f1:.4f}")
    print(f"Accuracy:        {accuracy:.4f}")

    print("\nPrediction distribution:")
    predictions.groupBy("prediction").count().show()

    print("\nConfusion matrix:")
    predictions.groupBy(
        LABEL_COLUMN,
        "prediction"
    ).count().orderBy(
        LABEL_COLUMN,
        "prediction"
    ).show()

    print("\nTraining Random Forest model...")

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol=LABEL_COLUMN,
        numTrees=20,
        maxDepth=8,
        seed=42
    )

    rf_model = rf.fit(train_df)

    print("\nRandom Forest Feature Importances:")

    feature_names = NUMERIC_FEATURES + ["type_encoded"]

    for name, importance in zip(
        feature_names,
        rf_model.featureImportances
    ):
        print(f"{name}: {importance:.6f}")

    rf_predictions = rf_model.transform(test_df)

    rf_pr_auc = pr_evaluator.evaluate(rf_predictions)
    rf_roc_auc = roc_evaluator.evaluate(rf_predictions)

    rf_confusion_rows = (
        rf_predictions
        .groupBy(LABEL_COLUMN, "prediction")
        .count()
        .collect()
    )

    rf_confusion = {
        (int(row[LABEL_COLUMN]), int(row["prediction"])): row["count"]
        for row in rf_confusion_rows
    }

    rf_tn = rf_confusion.get((0, 0), 0)
    rf_fp = rf_confusion.get((0, 1), 0)
    rf_fn = rf_confusion.get((1, 0), 0)
    rf_tp = rf_confusion.get((1, 1), 0)

    rf_precision = (
        rf_tp / (rf_tp + rf_fp)
        if (rf_tp + rf_fp)
        else 0.0
    )

    rf_recall = (
        rf_tp / (rf_tp + rf_fn)
        if (rf_tp + rf_fn)
        else 0.0
    )

    rf_f1 = (
        2 * rf_precision * rf_recall / (rf_precision + rf_recall)
        if (rf_precision + rf_recall)
        else 0.0
    )

    rf_accuracy = (
        (rf_tp + rf_tn) /
        (rf_tp + rf_tn + rf_fp + rf_fn)
    )

    print("\nRandom Forest Evaluation:")
    print(f"PR-AUC:          {rf_pr_auc:.4f}")
    print(f"ROC-AUC:         {rf_roc_auc:.4f}")
    print(f"Fraud Precision: {rf_precision:.4f}")
    print(f"Fraud Recall:    {rf_recall:.4f}")
    print(f"Fraud F1:        {rf_f1:.4f}")
    print(f"Accuracy:        {rf_accuracy:.4f}")

    print("\nRandom Forest Confusion Matrix:")
    rf_predictions.groupBy(
        LABEL_COLUMN,
        "prediction"
    ).count().orderBy(
        LABEL_COLUMN,
        "prediction"
    ).show()

    print("\nTraining set label distribution:")
    train_df.groupBy(LABEL_COLUMN).count().show()

    print("\nTest set label distribution:")
    test_df.groupBy(LABEL_COLUMN).count().show()
    
    print("\nML-ready training sample:")
    train_df.select(
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