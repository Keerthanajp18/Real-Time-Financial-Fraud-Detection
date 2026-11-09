"""
Member 4: Data Quality & Telemetry Module
Validates row counts at each Lakehouse layer (Bronze, Silver, Gold).

Paths come from LakehouseConfig (absolute, project-root based), so the checks
work from any working directory, including inside the Airflow container.
"""

from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession

from src.lakehouse.config import get_lakehouse_config
from src.logger import setup_logger

logger = setup_logger("lakehouse_telemetry")


def get_spark():
    """Get or create Spark session"""
    return SparkSession.builder \
        .appName("DataQualityCheck") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def _validate_layer(layer_name: str, layer_path: Path) -> Dict[str, Any]:
    """Shared check: path exists, is readable Parquet, and has at least one row."""
    logger.info(f"Validating {layer_name} Layer...")

    if not layer_path.exists():
        return {"passed": False, "error": f"Path not found: {layer_path}"}

    try:
        spark = get_spark()
        row_count = spark.read.parquet(str(layer_path)).count()

        if row_count == 0:
            return {"passed": False, "error": "Empty dataset"}

        logger.info(f"✅ {layer_name}: {row_count:,} records")
        return {"passed": True, "row_count": row_count}

    except Exception as e:
        logger.error(f"{layer_name} validation failed: {str(e)}")
        return {"passed": False, "error": str(e)}


def validate_bronze_layer() -> Dict[str, Any]:
    """Validate Bronze layer"""
    cfg = get_lakehouse_config()
    return _validate_layer("Bronze", Path(cfg.bronze_dir))


def validate_silver_layer() -> Dict[str, Any]:
    """Validate Silver layer"""
    cfg = get_lakehouse_config()
    return _validate_layer("Silver", Path(cfg.silver_dir))


def validate_gold_layer() -> Dict[str, Any]:
    """Validate Gold layer (enriched transactions, the Member 3 input)"""
    cfg = get_lakehouse_config()
    return _validate_layer("Gold", Path(cfg.gold_dir) / "enriched_transactions")
