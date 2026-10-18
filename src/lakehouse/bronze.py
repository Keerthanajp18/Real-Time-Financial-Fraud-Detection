"""
Bronze Layer Manager: Raw Ingestion Archive.
Preserves raw transaction payload with ingestion metadata in Snappy Parquet format.
"""
import uuid
from pathlib import Path
from typing import Dict, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.logger import setup_logger

logger = setup_logger("lakehouse_bronze")


class BronzeLayerManager:
    """
    Manages reading raw files/streams and loading them into the Bronze Parquet table.
    Ensures zero data loss and appends audit traceability metadata.
    """

    def __init__(self, config: Optional[LakehouseConfig] = None):
        self.config = config or get_lakehouse_config()
        self.output_path = self.config.bronze_dir

    def add_ingestion_metadata(
        self,
        df: DataFrame,
        source_name: str,
        batch_id: Optional[str] = None,
    ) -> DataFrame:
        """
        Appends audit metadata columns to raw incoming DataFrame:
          - ingest_batch_id: Unique identifier for the batch run
          - ingest_timestamp: Timestamp when Spark wrote to Bronze
          - source_system: Input source path or Kafka topic
          - record_hash: Hash of original row to detect duplicate raw deliveries
        """
        active_batch_id = batch_id or str(uuid.uuid4())
        cols = df.columns

        # Calculate record hash over all incoming raw columns
        hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)

        return (
            df.withColumn("ingest_batch_id", F.lit(active_batch_id))
            .withColumn("ingest_timestamp", F.current_timestamp())
            .withColumn("source_system", F.lit(str(source_name)))
            .withColumn("record_hash", hash_expr)
        )

    def write_bronze(
        self,
        df: DataFrame,
        source_name: str,
        mode: str = "append",
        batch_id: Optional[str] = None,
    ) -> Dict[str, Union[int, str]]:
        """
        Writes raw DataFrame into Bronze Parquet directory.
        
        Args:
            df: Raw Spark DataFrame.
            source_name: Description or path of source data.
            mode: Save mode ('append' or 'overwrite').
            batch_id: Optional batch ID string.
            
        Returns:
            Dictionary containing write metrics.
        """
        bronze_df = self.add_ingestion_metadata(df, source_name=source_name, batch_id=batch_id)
        
        # Count records before writing
        record_count = bronze_df.count()
        logger.info(f"Writing {record_count:,} records to Bronze Parquet at: {self.output_path} (mode={mode})")

        writer = bronze_df.write.mode(mode).option("compression", self.config.parquet_compression)

        if self.config.partition_by_step and "step" in bronze_df.columns:
            writer = writer.partitionBy("step")

        writer.parquet(self.output_path)
        logger.info(f"Successfully committed {record_count:,} records to Bronze layer.")

        return {
            "layer": "bronze",
            "records_written": record_count,
            "destination": self.output_path,
            "mode": mode,
        }

    def read_bronze(self, spark: SparkSession) -> DataFrame:
        """Reads the full Bronze Parquet dataset."""
        logger.debug(f"Reading Bronze dataset from: {self.output_path}")
        return spark.read.parquet(self.output_path)
