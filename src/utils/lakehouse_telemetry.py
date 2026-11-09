"""
Member 4: Data Quality & Telemetry Module
Validates row counts at each Lakehouse layer (Bronze, Silver, Gold).

Paths come from LakehouseConfig (absolute, project-root based), so the checks
work from any working directory, including inside the Airflow container.
"""

import time
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


# Restored from dad9932: Member 2's batch ETL (src/etl/batch_etl.py) still uses this class.
class LakehouseTelemetry:
    """
    Collects run statistics across Bronze, Silver, Gold, and Quarantine layers
    and outputs a standardized data engineering audit report.
    """

    def __init__(self, job_name: str = "lakehouse_etl"):
        self.job_name = job_name
        self.start_time = time.time()
        self.end_time = None
        self.metrics: Dict[str, Any] = {}

    def record_stage(self, stage_name: str, stage_metrics: Dict[str, Any]) -> None:
        """Records metrics from a pipeline stage."""
        self.metrics[stage_name] = stage_metrics

    def finalize(self) -> Dict[str, Any]:
        """Calculates total elapsed time and formats final report."""
        self.end_time = time.time()
        elapsed = round(self.end_time - self.start_time, 2)
        summary = {
            "job_name": self.job_name,
            "elapsed_seconds": elapsed,
            "stages": self.metrics,
        }
        return summary

    def format_cli_summary(self, summary: Dict[str, Any]) -> str:
        """Formats summary as a clean console table for MSc grading and monitoring."""
        lines = [
            "=" * 70,
            f"LAKEHOUSE ETL PIPELINE RUN SUMMARY ({summary.get('job_name')})",
            "=" * 70,
            f"  Total Duration: {summary.get('elapsed_seconds')}s",
        ]

        stages = summary.get("stages", {})
        if "extract" in stages:
            ext = stages["extract"]
            lines.append("\n[EXTRACT]")
            lines.append(f"  Input Source:     {ext.get('source')}")
            lines.append(f"  Extracted Rows:   {ext.get('records_read', 0):,}")

        if "bronze" in stages:
            brz = stages["bronze"]
            lines.append("\n[BRONZE LAYER]")
            lines.append(f"  Destination:      {brz.get('destination')}")
            lines.append(f"  Written Records:  {brz.get('records_written', 0):,}")
            lines.append(f"  Write Mode:       {brz.get('mode')}")

        if "silver" in stages:
            slv = stages["silver"]
            lines.append("\n[SILVER LAYER]")
            lines.append(f"  Valid Records:    {slv.get('silver_records', 0):,}")
            lines.append(f"  Quarantined:      {slv.get('quarantined_records', 0):,}")
            lines.append(f"  Duplicates Dropped:{slv.get('duplicates_removed', 0):,}")
            lines.append(f"  Silver Storage:   {slv.get('silver_destination')}")
            lines.append(f"  Quarantine Storage:{slv.get('quarantine_destination')}")

        if "gold" in stages:
            gld = stages["gold"]
            lines.append("\n[GOLD LAYER - DOWNSTREAM ANALYTICS & ML]")
            lines.append(f"  Enriched Features: {gld.get('enriched_transactions_count', 0):,} rows")
            lines.append(f"  Customer Profiles: {gld.get('customer_aggregates_count', 0):,} accounts")
            lines.append(f"  Hourly Rollups:    {gld.get('hourly_aggregates_count', 0):,} hours")
            lines.append(f"  Enriched Path:     {gld.get('enriched_path')}")

        lines.append("=" * 70)
        return "\n".join(lines)
