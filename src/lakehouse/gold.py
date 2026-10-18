"""
Gold Layer Manager: Analytics-Ready & Machine-Learning Datasets.
Prepares enriched feature tables, customer profile aggregations, and temporal metrics.
"""
from pathlib import Path
from typing import Dict, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.logger import setup_logger
from src.transformations.aggregations import (
    compute_customer_aggregates,
    compute_hourly_aggregates,
)
from src.transformations.features import build_gold_enriched_features

logger = setup_logger("lakehouse_gold")


class GoldLayerManager:
    """
    Manages generation and persistence of downstream analytics and ML feature tables in the Gold layer.
    """

    def __init__(self, config: Optional[LakehouseConfig] = None):
        self.config = config or get_lakehouse_config()
        self.gold_dir = Path(self.config.gold_dir)
        self.enriched_path = str(self.gold_dir / "enriched_transactions")
        self.customer_agg_path = str(self.gold_dir / "customer_aggregates")
        self.hourly_agg_path = str(self.gold_dir / "hourly_aggregates")

    def process_and_write(
        self,
        silver_df: DataFrame,
        mode: str = "overwrite",
    ) -> Dict[str, Union[int, str]]:
        """
        Transforms Silver cleaned data into Gold analytics tables:
          1. Enriched transactions with engineered ML features
          2. Customer behavioral summaries
          3. Time-based hourly aggregates
          
        Args:
            silver_df: Validated Silver DataFrame.
            mode: Write mode ('overwrite' or 'append').
            
        Returns:
            Dictionary with counts and paths of generated Gold tables.
        """
        logger.info("Starting Gold layer feature engineering and aggregation...")

        # 1. Build enriched transactions with ML features
        enriched_df = build_gold_enriched_features(silver_df)
        enriched_count = enriched_df.count()

        logger.info(f"Writing {enriched_count:,} enriched transactions to: {self.enriched_path}")
        writer_enriched = (
            enriched_df.write.mode(mode)
            .option("compression", self.config.parquet_compression)
        )
        if self.config.partition_by_step and "step" in enriched_df.columns:
            writer_enriched = writer_enriched.partitionBy("step")
        writer_enriched.parquet(self.enriched_path)

        # 2. Build customer profile aggregations
        customer_agg_df = compute_customer_aggregates(enriched_df)
        customer_count = customer_agg_df.count()

        logger.info(f"Writing {customer_count:,} customer profiles to: {self.customer_agg_path}")
        (
            customer_agg_df.write.mode(mode)
            .option("compression", self.config.parquet_compression)
            .parquet(self.customer_agg_path)
        )

        # 3. Build hourly temporal aggregations
        hourly_agg_df = compute_hourly_aggregates(enriched_df)
        hourly_count = hourly_agg_df.count()

        logger.info(f"Writing {hourly_count:,} hourly aggregate rows to: {self.hourly_agg_path}")
        (
            hourly_agg_df.write.mode(mode)
            .option("compression", self.config.parquet_compression)
            .parquet(self.hourly_agg_path)
        )

        logger.info(
            f"Gold Layer Generation Complete:\n"
            f"  - Enriched Transactions: {enriched_count:,} records -> {self.enriched_path}\n"
            f"  - Customer Profiles:     {customer_count:,} accounts -> {self.customer_agg_path}\n"
            f"  - Hourly Aggregates:     {hourly_count:,} steps    -> {self.hourly_agg_path}"
        )

        return {
            "layer": "gold",
            "enriched_transactions_count": enriched_count,
            "customer_aggregates_count": customer_count,
            "hourly_aggregates_count": hourly_count,
            "enriched_path": self.enriched_path,
            "customer_agg_path": self.customer_agg_path,
            "hourly_agg_path": self.hourly_agg_path,
            "mode": mode,
        }

    def read_enriched_transactions(self, spark: SparkSession) -> DataFrame:
        """Reads Gold enriched transactions table (Primary interface for Member 3 ML)."""
        logger.debug(f"Reading Gold enriched transactions from: {self.enriched_path}")
        return spark.read.parquet(self.enriched_path)

    def read_customer_aggregates(self, spark: SparkSession) -> DataFrame:
        """Reads Gold customer profile aggregates."""
        logger.debug(f"Reading Gold customer aggregates from: {self.customer_agg_path}")
        return spark.read.parquet(self.customer_agg_path)

    def read_hourly_aggregates(self, spark: SparkSession) -> DataFrame:
        """Reads Gold hourly temporal metrics."""
        logger.debug(f"Reading Gold hourly aggregates from: {self.hourly_agg_path}")
        return spark.read.parquet(self.hourly_agg_path)
