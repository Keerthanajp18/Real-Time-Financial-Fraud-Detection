"""
Batch Lakehouse ETL Pipeline: Bronze -> Silver -> Gold.
Orchestrates batch extraction from CSV/JSON, schema conformance, quarantine routing,
deduplication, and feature engineering.
"""
import time
from pathlib import Path
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from src.lakehouse.bronze import BronzeLayerManager
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.gold import GoldLayerManager
from src.lakehouse.silver import SilverLayerManager
from src.lakehouse.spark_session import get_spark_session
from src.logger import setup_logger

logger = setup_logger("batch_etl")

# Telemetry integration: Try importing LakehouseTelemetry if available in utils,
# otherwise provide built-in telemetry collector aligned with project architecture.
try:
    from src.utils.lakehouse_telemetry import LakehouseTelemetry
except (ImportError, ModuleNotFoundError):
    class LakehouseTelemetry:  # type: ignore[no-redef]
        """
        Internal telemetry and audit logging utility for Lakehouse ETL pipelines.
        Collects run statistics across Bronze, Silver, Gold, and Quarantine layers
        and formats standardized audit reports aligned with src.logger.
        """

        def __init__(self, job_name: str = "lakehouse_etl"):
            self.job_name = job_name
            self.start_time = time.time()
            self.end_time: Optional[float] = None
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
            """Formats summary as a clean console report table for monitoring and logs."""
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


class BatchLakehouseETL:
    """
    Modular, reusable batch pipeline that reads input data and transforms it
    through Bronze, Silver, and Gold Lakehouse layers.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[LakehouseConfig] = None,
    ):
        self.config = config or get_lakehouse_config()
        self.spark = spark or get_spark_session(
            app_name=self.config.spark_app_name,
            master=self.config.spark_master,
        )
        self.bronze_manager = BronzeLayerManager(self.config)
        self.silver_manager = SilverLayerManager(self.config)
        self.gold_manager = GoldLayerManager(self.config)

    def extract(self, input_path: str, file_format: str = "csv") -> DataFrame:
        """
        Extracts raw transaction data from a file (CSV, JSON, or Parquet).
        
        Args:
            input_path: Path to dataset file or folder.
            file_format: 'csv', 'json', or 'parquet'.
            
        Returns:
            Extracted raw Spark DataFrame.
        """
        resolved = str(self.config.resolved_path(input_path))
        logger.info(f"Extracting raw data from: {resolved} (format={file_format})")

        fmt = file_format.lower()
        if fmt == "csv":
            df = self.spark.read.option("header", "true").option("inferSchema", "false").csv(resolved)
        elif fmt == "json":
            df = self.spark.read.json(resolved)
        elif fmt == "parquet":
            df = self.spark.read.parquet(resolved)
        else:
            raise ValueError(f"Unsupported file format: '{file_format}'. Supported formats: csv, json, parquet")

        return df

    def run(
        self,
        input_path: Optional[str] = None,
        file_format: str = "csv",
        bronze_mode: str = "append",
        silver_mode: str = "append",
        gold_mode: str = "overwrite",
        max_records: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Executes the full End-to-End Batch ETL:
          Extract -> Bronze -> Silver -> Gold
          
        Args:
            input_path: Input file path (defaults to config.default_input_dataset).
            file_format: Format of input file ('csv', 'json', 'parquet').
            bronze_mode: Write mode for Bronze ('append' or 'overwrite').
            silver_mode: Write mode for Silver ('append' or 'overwrite').
            gold_mode: Write mode for Gold ('overwrite' or 'append').
            max_records: Optional limit on number of rows to ingest.
            
        Returns:
            Execution summary dictionary with complete layer telemetry.
        """
        source = input_path or self.config.default_input_dataset
        telemetry = LakehouseTelemetry(job_name="BatchLakehouseETL")

        logger.info(f"Starting Batch Lakehouse ETL Pipeline from source: {source}")

        # Stage 1: Extract
        raw_df = self.extract(input_path=source, file_format=file_format)
        if max_records and max_records > 0:
            logger.info(f"Limiting extract to top {max_records:,} records.")
            raw_df = raw_df.limit(max_records)

        extracted_count = raw_df.count()
        telemetry.record_stage("extract", {
            "source": source,
            "format": file_format,
            "records_read": extracted_count,
        })

        if extracted_count == 0:
            logger.warning("Extracted dataset is empty. Pipeline halted gracefully.")
            return telemetry.finalize()

        # Stage 2: Bronze Layer (Raw Ingestion)
        bronze_metrics = self.bronze_manager.write_bronze(
            df=raw_df,
            source_name=source,
            mode=bronze_mode,
        )
        telemetry.record_stage("bronze", bronze_metrics)

        # Stage 3: Silver Layer (Clean, Validate, Quarantine, Deduplicate)
        # Read from Bronze to ensure decoupled repeatability
        bronze_persisted_df = self.bronze_manager.read_bronze(self.spark)
        silver_metrics = self.silver_manager.process_and_write(
            df=bronze_persisted_df,
            mode=silver_mode,
        )
        telemetry.record_stage("silver", silver_metrics)

        # Stage 4: Gold Layer (ML-Ready Enriched Features & Aggregates)
        silver_persisted_df = self.silver_manager.read_silver(self.spark)
        gold_metrics = self.gold_manager.process_and_write(
            silver_df=silver_persisted_df,
            mode=gold_mode,
        )
        telemetry.record_stage("gold", gold_metrics)

        summary = telemetry.finalize()
        logger.info("\n" + telemetry.format_cli_summary(summary))
        return summary

