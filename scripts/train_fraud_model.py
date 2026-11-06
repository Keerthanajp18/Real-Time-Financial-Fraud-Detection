import json
import os
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier,
)
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
        .master(os.getenv("SPARK_MASTER", "local[*]"))
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

    model_train_df, validation_df = train_df.randomSplit(
        [0.8, 0.2],
        seed=123
    )

    print("\nThreshold-tuning split:")
    print(f"Model training rows: {model_train_df.count():,}")
    print(f"Validation rows:     {validation_df.count():,}")

    print("\nModel training class distribution:")
    model_train_df.groupBy(LABEL_COLUMN).count().orderBy(LABEL_COLUMN).show()

    print("\nValidation class distribution:")
    validation_df.groupBy(LABEL_COLUMN).count().orderBy(LABEL_COLUMN).show()

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

    print("\nTraining Safe-Feature Logistic Regression...")

    safe_lr = LogisticRegression(
        featuresCol="safe_features",
        labelCol=LABEL_COLUMN,
        maxIter=20
    )

    safe_lr_model = safe_lr.fit(train_df)

    safe_lr_predictions = safe_lr_model.transform(test_df)

    safe_lr_pr_auc = pr_evaluator.evaluate(safe_lr_predictions)
    safe_lr_roc_auc = roc_evaluator.evaluate(safe_lr_predictions)

    safe_lr_confusion_rows = (
        safe_lr_predictions
        .groupBy(LABEL_COLUMN, "prediction")
        .count()
        .collect()
    )

    safe_lr_confusion = {
        (int(row[LABEL_COLUMN]), int(row["prediction"])): row["count"]
        for row in safe_lr_confusion_rows
    }

    safe_lr_tn = safe_lr_confusion.get((0, 0), 0)
    safe_lr_fp = safe_lr_confusion.get((0, 1), 0)
    safe_lr_fn = safe_lr_confusion.get((1, 0), 0)
    safe_lr_tp = safe_lr_confusion.get((1, 1), 0)

    safe_lr_precision = (
        safe_lr_tp / (safe_lr_tp + safe_lr_fp)
        if (safe_lr_tp + safe_lr_fp)
        else 0.0
    )

    safe_lr_recall = (
        safe_lr_tp / (safe_lr_tp + safe_lr_fn)
        if (safe_lr_tp + safe_lr_fn)
        else 0.0
    )

    safe_lr_f1 = (
        2 * safe_lr_precision * safe_lr_recall
        / (safe_lr_precision + safe_lr_recall)
        if (safe_lr_precision + safe_lr_recall)
        else 0.0
    )

    safe_lr_accuracy = (
        (safe_lr_tp + safe_lr_tn)
        / (safe_lr_tp + safe_lr_tn + safe_lr_fp + safe_lr_fn)
    )

    print("\nSafe-Feature Logistic Regression Evaluation:")
    print(f"PR-AUC:          {safe_lr_pr_auc:.4f}")
    print(f"ROC-AUC:         {safe_lr_roc_auc:.4f}")
    print(f"Fraud Precision: {safe_lr_precision:.4f}")
    print(f"Fraud Recall:    {safe_lr_recall:.4f}")
    print(f"Fraud F1:        {safe_lr_f1:.4f}")
    print(f"Accuracy:        {safe_lr_accuracy:.4f}")

    print("\nSafe-Feature Logistic Regression Confusion Matrix:")
    safe_lr_predictions.groupBy(
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

    print("\nTraining Safe-Feature Gradient-Boosted Trees model...")

    safe_gbt = GBTClassifier(
        featuresCol="safe_features",
        labelCol=LABEL_COLUMN,
        maxIter=20,
        maxDepth=5,
        seed=42
    )

    safe_gbt_model = safe_gbt.fit(train_df)

    safe_gbt_predictions = safe_gbt_model.transform(test_df)

    safe_gbt_pr_auc = pr_evaluator.evaluate(safe_gbt_predictions)
    safe_gbt_roc_auc = roc_evaluator.evaluate(safe_gbt_predictions)

    safe_gbt_confusion_rows = (
        safe_gbt_predictions
        .groupBy(LABEL_COLUMN, "prediction")
        .count()
        .collect()
    )

    safe_gbt_confusion = {
        (int(row[LABEL_COLUMN]), int(row["prediction"])): row["count"]
        for row in safe_gbt_confusion_rows
    }

    safe_gbt_tn = safe_gbt_confusion.get((0, 0), 0)
    safe_gbt_fp = safe_gbt_confusion.get((0, 1), 0)
    safe_gbt_fn = safe_gbt_confusion.get((1, 0), 0)
    safe_gbt_tp = safe_gbt_confusion.get((1, 1), 0)

    safe_gbt_precision = (
        safe_gbt_tp / (safe_gbt_tp + safe_gbt_fp)
        if (safe_gbt_tp + safe_gbt_fp)
        else 0.0
    )

    safe_gbt_recall = (
        safe_gbt_tp / (safe_gbt_tp + safe_gbt_fn)
        if (safe_gbt_tp + safe_gbt_fn)
        else 0.0
    )

    safe_gbt_f1 = (
        2 * safe_gbt_precision * safe_gbt_recall
        / (safe_gbt_precision + safe_gbt_recall)
        if (safe_gbt_precision + safe_gbt_recall)
        else 0.0
    )

    safe_gbt_accuracy = (
        (safe_gbt_tp + safe_gbt_tn)
        / (safe_gbt_tp + safe_gbt_tn + safe_gbt_fp + safe_gbt_fn)
    )

    print("\nSafe-Feature GBT Evaluation:")
    print(f"PR-AUC:          {safe_gbt_pr_auc:.4f}")
    print(f"ROC-AUC:         {safe_gbt_roc_auc:.4f}")
    print(f"Fraud Precision: {safe_gbt_precision:.4f}")
    print(f"Fraud Recall:    {safe_gbt_recall:.4f}")
    print(f"Fraud F1:        {safe_gbt_f1:.4f}")
    print(f"Accuracy:        {safe_gbt_accuracy:.4f}")

    print("\nSafe-Feature GBT Confusion Matrix:")
    safe_gbt_predictions.groupBy(
        LABEL_COLUMN,
        "prediction"
    ).count().orderBy(
        LABEL_COLUMN,
        "prediction"
    ).show()

    print("\nTraining GBT for threshold tuning...")

    tuning_gbt = GBTClassifier(
        featuresCol="safe_features",
        labelCol=LABEL_COLUMN,
        maxIter=20,
        maxDepth=5,
        seed=42
    )

    tuning_gbt_model = tuning_gbt.fit(model_train_df)

    validation_predictions = tuning_gbt_model.transform(validation_df)

    validation_predictions = validation_predictions.withColumn(
        "fraud_probability",
        vector_to_array("probability")[1]
    )

    validation_predictions.cache()

    thresholds = [0.1, 0.2, 0.3, 0.4, 0.5]

    print("\n=== GBT THRESHOLD TUNING ON VALIDATION SET ===")

    for threshold in thresholds:

        threshold_predictions = validation_predictions.withColumn(
            "threshold_prediction",
            F.when(
                F.col("fraud_probability") >= threshold,
                1.0
            ).otherwise(0.0)
        )

        rows = (
            threshold_predictions
            .groupBy(LABEL_COLUMN, "threshold_prediction")
            .count()
            .collect()
        )

        confusion = {
            (
                int(row[LABEL_COLUMN]),
                int(row["threshold_prediction"])
            ): row["count"]
            for row in rows
        }

        tn = confusion.get((0, 0), 0)
        fp = confusion.get((0, 1), 0)
        fn = confusion.get((1, 0), 0)
        tp = confusion.get((1, 1), 0)

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0

        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall)
            else 0.0
        )

        print(
            f"Threshold={threshold:.1f} | "
            f"Precision={precision:.4f} | "
            f"Recall={recall:.4f} | "
            f"F1={f1:.4f} | "
            f"TP={tp} | FP={fp} | FN={fn}"
        )

    final_threshold = 0.4

    print(
        f"\n=== FINAL SAFE GBT TEST EVALUATION "
        f"(threshold={final_threshold}) ==="
    )

    final_test_predictions = safe_gbt_predictions.withColumn(
        "fraud_probability",
        vector_to_array("probability")[1]
    ).withColumn(
        "final_prediction",
        F.when(
            F.col("fraud_probability") >= final_threshold,
            1.0
        ).otherwise(0.0)
    )

    final_rows = (
        final_test_predictions
        .groupBy(LABEL_COLUMN, "final_prediction")
        .count()
        .collect()
    )

    final_confusion = {
        (
            int(row[LABEL_COLUMN]),
            int(row["final_prediction"])
        ): row["count"]
        for row in final_rows
    }

    final_tn = final_confusion.get((0, 0), 0)
    final_fp = final_confusion.get((0, 1), 0)
    final_fn = final_confusion.get((1, 0), 0)
    final_tp = final_confusion.get((1, 1), 0)

    final_precision = (
        final_tp / (final_tp + final_fp)
        if (final_tp + final_fp)
        else 0.0
    )

    final_recall = (
        final_tp / (final_tp + final_fn)
        if (final_tp + final_fn)
        else 0.0
    )

    final_f1 = (
        2 * final_precision * final_recall
        / (final_precision + final_recall)
        if (final_precision + final_recall)
        else 0.0
    )

    print(f"Fraud Precision: {final_precision:.4f}")
    print(f"Fraud Recall:    {final_recall:.4f}")
    print(f"Fraud F1:        {final_f1:.4f}")
    print(f"TP={final_tp} | FP={final_fp} | FN={final_fn} | TN={final_tn}")

    validation_predictions.unpersist()

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

    print("\nSaving final Member 3 model artifacts...")

    model_dir = "models/member3"
    os.makedirs(model_dir, exist_ok=True)

    pipeline_path = f"{model_dir}/preprocessing_pipeline"
    gbt_model_path = f"{model_dir}/safe_gbt_model"
    config_path = f"{model_dir}/model_config.json"

    pipeline_model.write().overwrite().save(pipeline_path)
    safe_gbt_model.write().overwrite().save(gbt_model_path)

    model_config = {
        "model_type": "GBTClassifier",
        "threshold": 0.4,
        "features": SAFE_NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "label": LABEL_COLUMN,
        "test_precision": final_precision,
        "test_recall": final_recall,
        "test_f1": final_f1,
    }

    with open(config_path, "w") as f:
        json.dump(model_config, f, indent=4)

    print(f"Preprocessing pipeline saved to: {pipeline_path}")
    print(f"GBT model saved to:              {gbt_model_path}")
    print(f"Model configuration saved to:    {config_path}")

    spark.stop()

if __name__ == "__main__":
    main()