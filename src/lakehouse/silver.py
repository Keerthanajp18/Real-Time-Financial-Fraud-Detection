"""
Silver Layer Manager: Cleaned, Conformed, Deduplicated, and Quarantined Data.
"""
from typing import Dict, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.logger import setup_logger
from src.transformations.cleaning import (
    deduplicate_transactions,
    validate_and_split_records,
)

logger = setup_logger("lakehouse_silver")


class SilverLayerManager:
    """
    Manages transformation from Bronze raw records to Silver validated records,
    routes bad data to the quarantine table, and performs deduplication.
    """

    def __init__(self, config: Optional[LakehouseConfig] = None):
        self.config = config or get_lakehouse_config()
        self.silver_path = self.config.silver_dir
        self.quarantine_path = self.config.quarantine_dir

    def process_and_write(
        self,
        df: DataFrame,
        mode: str = "append",
    ) -> Dict[str, Union[int, str]]:
        """
        Processes Bronze data:
          1. Validates schema and values
          2. Routes rejected records to Quarantine
          3. Deduplicates valid records
          4. Writes clean records to Silver Parquet
          
        Args:
            df: Input DataFrame (typically read from Bronze).
            mode: Write mode ('append' or 'overwrite').
            
        Returns:
            Dictionary with counts of input, valid, quarantined, and deduplicated records.
        """
        total_input = df.count()
        logger.info(f"Starting Silver layer processing for {total_input:,} input records...")

        # 1. Validate & split records
        valid_df, quarantined_df = validate_and_split_records(df)

        # 2. Write quarantined records if any exist
        quarantined_count = quarantined_df.count()
        if quarantined_count > 0:
            logger.warning(
                f"Quarantine Alert: {quarantined_count:,} records failed validation rules. "
                f"Writing to: {self.quarantine_path}"
            )
            (
                quarantined_df.write.mode("append")
                .option("compression", self.config.parquet_compression)
                .parquet(self.quarantine_path)
            )
        else:
            logger.info("Validation passed: 0 records required quarantine.")

        # 3. Deduplicate valid records
        dedup_df = deduplicate_transactions(valid_df)
        silver_count = dedup_df.count()
        duplicates_removed = (total_input - quarantined_count) - silver_count

        logger.info(
            f"Silver Quality Summary:\n"
            f"  - Total Ingested:      {total_input:,}\n"
            f"  - Passed Validation:   {total_input - quarantined_count:,}\n"
            f"  - Quarantined:         {quarantined_count:,}\n"
            f"  - Duplicates Removed:  {duplicates_removed:,}\n"
            f"  - Clean Silver Output: {silver_count:,}"
        )

        # 4. Write clean Silver records
        writer = dedup_df.write.mode(mode).option("compression", self.config.parquet_compression)
        if self.config.partition_by_step and "step" in dedup_df.columns:
            writer = writer.partitionBy("step")

        writer.parquet(self.silver_path)
        logger.info(f"Successfully saved {silver_count:,} records to Silver layer at: {self.silver_path}")

        return {
            "layer": "silver",
            "total_input": total_input,
            "silver_records": silver_count,
            "quarantined_records": quarantined_count,
            "duplicates_removed": duplicates_removed,
            "silver_destination": self.silver_path,
            "quarantine_destination": self.quarantine_path,
            "mode": mode,
        }

    def read_silver(self, spark: SparkSession) -> DataFrame:
        """Reads the full Silver Parquet dataset."""
        logger.debug(f"Reading Silver dataset from: {self.silver_path}")
        return spark.read.parquet(self.silver_path)

    def read_quarantine(self, spark: SparkSession) -> DataFrame:
        """Reads quarantined invalid transactions."""
        logger.debug(f"Reading Quarantine dataset from: {self.quarantine_path}")
        return spark.read.parquet(self.quarantine_path)
