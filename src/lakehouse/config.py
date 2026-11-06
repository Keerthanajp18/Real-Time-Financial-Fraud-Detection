import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Load .env file if available
load_dotenv()

# Base project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class LakehouseConfig(BaseModel):
    """
    Configuration parameters for Bronze, Silver, Gold, and Quarantine layers,
    ETL batch and streaming jobs, and Apache Spark sessions.
    Supports environment variables with sensible defaults.
    """
    # Base storage directory
    data_dir: str = Field(
        default_factory=lambda: os.getenv("LAKEHOUSE_DATA_DIR", str(PROJECT_ROOT / "data")),
        description="Root directory for lakehouse Parquet storage."
    )

    # Layer subdirectories
    bronze_dir: str = Field(
        default_factory=lambda: os.getenv("BRONZE_DATA_DIR", str(PROJECT_ROOT / "data" / "bronze" / "transactions")),
        description="Path to store Bronze raw transactions (Parquet)."
    )
    silver_dir: str = Field(
        default_factory=lambda: os.getenv("SILVER_DATA_DIR", str(PROJECT_ROOT / "data" / "silver" / "transactions")),
        description="Path to store Silver cleaned transactions (Parquet)."
    )
    quarantine_dir: str = Field(
        default_factory=lambda: os.getenv("QUARANTINE_DATA_DIR", str(PROJECT_ROOT / "data" / "quarantine" / "transactions")),
        description="Path to store quarantined / rejected invalid transactions."
    )
    gold_dir: str = Field(
        default_factory=lambda: os.getenv("GOLD_DATA_DIR", str(PROJECT_ROOT / "data" / "gold")),
        description="Base directory for Gold analytics and ML datasets."
    )
    checkpoint_dir: str = Field(
        default_factory=lambda: os.getenv("CHECKPOINT_DIR", str(PROJECT_ROOT / "data" / "checkpoints")),
        description="Directory for Spark Structured Streaming checkpointing."
    )

    # Input sources
    default_input_dataset: str = Field(
        default_factory=lambda: os.getenv("DATASET_PATH", str(PROJECT_ROOT / "dataset" / "paysim.csv")),
        description="Default CSV input file path for batch ETL."
    )

    # Kafka streaming sources
    kafka_bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        description="Kafka bootstrap server(s)."
    )
    kafka_topic: str = Field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "financial-transactions"),
        description="Kafka topic containing ingested financial transactions."
    )
    kafka_starting_offsets: str = Field(
        default_factory=lambda: os.getenv("KAFKA_STARTING_OFFSETS", "latest"),
        description="Kafka starting offsets (earliest or latest)."
    )

    # Spark configuration settings
    spark_app_name: str = Field(
        default_factory=lambda: os.getenv("SPARK_APP_NAME", "FraudDetectionLakehouse"),
        description="Spark application name."
    )
    spark_master: str = Field(
        default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"),
        description="Spark master URL."
    )
    spark_driver_memory: str = Field(
        default_factory=lambda: os.getenv("SPARK_DRIVER_MEMORY", "2g"),
        description="Spark driver memory allocation."
    )
    parquet_compression: str = Field(
        default_factory=lambda: os.getenv("PARQUET_COMPRESSION", "snappy"),
        description="Parquet compression codec (snappy, gzip, zstd)."
    )
    partition_by_step: bool = Field(
        default_factory=lambda: os.getenv("PARTITION_BY_STEP", "true").lower() in ("true", "1", "yes"),
        description="Whether to partition Bronze/Silver tables by simulation step."
    )

    def resolved_path(self, relative_or_absolute: str) -> Path:
        """Helper to resolve a path against PROJECT_ROOT if it is not absolute."""
        p = Path(relative_or_absolute)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        return p


def get_lakehouse_config(**overrides) -> LakehouseConfig:
    """Factory function returning LakehouseConfig with optional runtime overrides."""
    return LakehouseConfig(**overrides)
