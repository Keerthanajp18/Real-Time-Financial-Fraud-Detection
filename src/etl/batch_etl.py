"""
Batch Lakehouse ETL Pipeline: Bronze -> Silver -> Gold.
Orchestrates batch extraction from CSV/JSON, schema conformance, quarantine routing,
deduplication, and feature engineering.
"""
from pathlib import Path
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from src.lakehouse.bronze import BronzeLayerManager
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.gold import GoldLayerManager
from src.lakehouse.silver import SilverLayerManager
from src.lakehouse.spark_session import get_spark_session
from src.logger import setup_logger
from src.utils.lakehouse_telemetry import LakehouseTelemetry

logger = setup_logger("batch_etl")


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
