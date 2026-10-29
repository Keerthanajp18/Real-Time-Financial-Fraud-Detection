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

RESTRICTED_NUMERIC_FEATURES = [
    "amount",
    "is_high_risk_type",
    "is_merchant_dest",
    "is_customer_dest",
    "is_large_transaction",
    "step",
]

SAFE_NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg",
    "oldbalanceDest",
    "is_high_risk_type",
    "is_merchant_dest",
    "is_customer_dest",
    "orig_zero_balance_flag",
    "dest_zero_balance_flag",
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

    restricted_assembler = VectorAssembler(
        inputCols=RESTRICTED_NUMERIC_FEATURES + ["type_encoded"],
        outputCol="restricted_features"
    )

    safe_assembler = VectorAssembler(
        inputCols=SAFE_NUMERIC_FEATURES + ["type_encoded"],
        outputCol="safe_features"
    )

    pipeline = Pipeline(
        stages=[
            type_indexer,
            type_encoder,
            assembler,
            restricted_assembler,
            safe_assembler
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

    print("\nTraining Restricted Random Forest model...")

    restricted_rf = RandomForestClassifier(
        featuresCol="restricted_features",
        labelCol=LABEL_COLUMN,
        numTrees=20,
        maxDepth=8,
        seed=42
    )

    restricted_rf_model = restricted_rf.fit(train_df)

    restricted_predictions = restricted_rf_model.transform(test_df)

    restricted_pr_auc = pr_evaluator.evaluate(restricted_predictions)
    restricted_roc_auc = roc_evaluator.evaluate(restricted_predictions)

    restricted_confusion_rows = (
        restricted_predictions
        .groupBy(LABEL_COLUMN, "prediction")
        .count()
        .collect()
    )

    restricted_confusion = {
        (int(row[LABEL_COLUMN]), int(row["prediction"])): row["count"]
        for row in restricted_confusion_rows
    }

    restricted_tn = restricted_confusion.get((0, 0), 0)
    restricted_fp = restricted_confusion.get((0, 1), 0) 
    restricted_fn = restricted_confusion.get((1, 0), 0)
    restricted_tp = restricted_confusion.get((1, 1), 0)

    restricted_precision = (
        restricted_tp / (restricted_tp + restricted_fp)
        if (restricted_tp + restricted_fp)
        else 0.0
    )

    restricted_recall = (
        restricted_tp / (restricted_tp + restricted_fn)
        if (restricted_tp + restricted_fn)
        else 0.0
    )

    restricted_f1 = (
        2 * restricted_precision * restricted_recall
        / (restricted_precision + restricted_recall)
        if (restricted_precision + restricted_recall)
        else 0.0
    )

    restricted_accuracy = (
        (restricted_tp + restricted_tn)
        / (
            restricted_tp
            + restricted_tn
            + restricted_fp
            + restricted_fn
        )
    )

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

    print("\nRestricted Random Forest Evaluation:")
    print(f"PR-AUC:          {restricted_pr_auc:.4f}")
    print(f"ROC-AUC:         {restricted_roc_auc:.4f}")
    print(f"Fraud Precision: {restricted_precision:.4f}")
    print(f"Fraud Recall:    {restricted_recall:.4f}")
    print(f"Fraud F1:        {restricted_f1:.4f}")
    print(f"Accuracy:        {restricted_accuracy:.4f}")

    print("\nRestricted Random Forest Confusion Matrix:")
    restricted_predictions.groupBy(
        LABEL_COLUMN,
        "prediction"
    ).count().orderBy(
        LABEL_COLUMN,
        "prediction"
    ).show()

    print("\nRandom Forest Confusion Matrix:")
    rf_predictions.groupBy(
        LABEL_COLUMN,
        "prediction"
    ).count().orderBy(
        LABEL_COLUMN,
        "prediction"
    ).show()

    print("\nTraining Safe-Feature Random Forest model...")

    safe_rf = RandomForestClassifier(
        featuresCol="safe_features",
        labelCol=LABEL_COLUMN,
        numTrees=20,
        maxDepth=8,
        seed=42
    )

    safe_rf_model = safe_rf.fit(train_df)

    safe_predictions = safe_rf_model.transform(test_df)

    safe_pr_auc = pr_evaluator.evaluate(safe_predictions)
    safe_roc_auc = roc_evaluator.evaluate(safe_predictions)

    safe_confusion_rows = (
        safe_predictions
        .groupBy(LABEL_COLUMN, "prediction")
        .count()
        .collect()
    )

    safe_confusion = {
        (int(row[LABEL_COLUMN]), int(row["prediction"])): row["count"]
        for row in safe_confusion_rows
    }

    safe_tn = safe_confusion.get((0, 0), 0)
    safe_fp = safe_confusion.get((0, 1), 0)
    safe_fn = safe_confusion.get((1, 0), 0)
    safe_tp = safe_confusion.get((1, 1), 0)

    safe_precision = (
        safe_tp / (safe_tp + safe_fp)
        if (safe_tp + safe_fp)
        else 0.0
    )

    safe_recall = (
        safe_tp / (safe_tp + safe_fn)
        if (safe_tp + safe_fn)
        else 0.0
    )

    safe_f1 = (
        2 * safe_precision * safe_recall
        / (safe_precision + safe_recall)
        if (safe_precision + safe_recall)
        else 0.0
    )

    safe_accuracy = (
        (safe_tp + safe_tn)
        / (safe_tp + safe_tn + safe_fp + safe_fn)
        if (safe_tp + safe_tn + safe_fp + safe_fn)
        else 0.0
    )

    print("\nSafe-Feature Random Forest Evaluation:")
    print(f"PR-AUC:          {safe_pr_auc:.4f}")
    print(f"ROC-AUC:         {safe_roc_auc:.4f}")
    print(f"Fraud Precision: {safe_precision:.4f}")
    print(f"Fraud Recall:    {safe_recall:.4f}")
    print(f"Fraud F1:        {safe_f1:.4f}")
    print(f"Accuracy:        {safe_accuracy:.4f}")

    print("\nSafe-Feature Random Forest Confusion Matrix:")
    safe_predictions.groupBy(
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