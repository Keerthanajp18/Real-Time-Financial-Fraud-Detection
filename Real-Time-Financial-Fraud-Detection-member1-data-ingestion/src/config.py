import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Load .env file if available
load_dotenv()

# Project root path (assumed to be the repository root directory)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


class IngestionConfig(BaseModel):
    """
    Configuration parameters for batch ingestion and Kafka streaming.
    All attributes support environment variable overrides.
    """
    dataset_path: str = Field(
        default_factory=lambda: os.getenv("DATASET_PATH", str(PROJECT_ROOT / "dataset" / "paysim.csv")),
        description="Path to the PaySim CSV dataset file."
    )
    kafka_bootstrap_servers: str = Field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        description="Kafka broker bootstrap servers."
    )
    kafka_topic: str = Field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "financial-transactions"),
        description="Kafka topic for valid transaction streaming."
    )
    kafka_dlq_topic: str = Field(
        default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", "financial-transactions-dlq"),
        description="Dead Letter Queue topic for invalid records."
    )
    batch_size: int = Field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "5000")),
        description="Number of records per streaming batch chunk."
    )
    streaming_delay_seconds: float = Field(
        default_factory=lambda: float(os.getenv("STREAMING_DELAY_SECONDS", "0.0")),
        description="Delay in seconds between records to simulate real-time streaming."
    )
    events_per_second: float = Field(
        default_factory=lambda: float(os.getenv("EVENTS_PER_SECOND", "0.0")),
        description="Target throughput in events per second (0.0 means unconstrained or controlled by delay)."
    )
    max_records: int = Field(
        default_factory=lambda: int(os.getenv("MAX_RECORDS", "0")),
        description="Maximum records to process (0 means process all records)."
    )
    kafka_dry_run: bool = Field(
        default_factory=lambda: os.getenv("KAFKA_DRY_RUN", "false").lower() in ("true", "1", "yes"),
        description="If True, skips sending to actual Kafka broker for offline verification."
    )
    log_level: str = Field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"),
        description="Logging level."
    )

    def resolved_dataset_path(self) -> Path:
        """Returns the dataset path resolved against PROJECT_ROOT if relative."""
        p = Path(self.dataset_path)
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        return p


def get_config(**overrides) -> IngestionConfig:
    """Helper factory function to get IngestionConfig with optional overrides."""
    return IngestionConfig(**overrides)
