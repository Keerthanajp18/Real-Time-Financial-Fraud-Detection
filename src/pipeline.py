import time
from typing import Any, Callable, Dict, Optional
from src.config import IngestionConfig
from src.ingestion.reader import IncrementalCSVReader
from src.logger import setup_logger
from src.models.transaction import validate_raw_record
from src.producer.kafka_producer import TransactionKafkaProducer
from src.producer.rate_limiter import StreamingRateLimiter

logger = setup_logger("ingestion_pipeline")


class IngestionPipeline:
    """
    Orchestrates batch ingestion, data validation, rate pacing, and Kafka publishing.
    """

    def __init__(
        self,
        config: IngestionConfig,
        producer: Optional[TransactionKafkaProducer] = None,
    ):
        self.config = config
        self._custom_producer = producer
        self._stop_requested = False

        # Ingestion metrics
        self.total_processed = 0
        self.valid_count = 0
        self.invalid_count = 0
        self.fraud_count = 0
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    def request_stop(self) -> None:
        """Signals the pipeline to stop gracefully after current batch."""
        logger.info("Graceful shutdown requested for IngestionPipeline.")
        self._stop_requested = True

    def run(self) -> Dict[str, Any]:
        """
        Executes the ingestion pipeline.
        Processes records incrementally, validates each row, and publishes to Kafka.
        """
        resolved_path = self.config.resolved_dataset_path()
        logger.info(
            f"Starting Ingestion Pipeline:\n"
            f"  - Dataset Path: {resolved_path}\n"
            f"  - Kafka Servers: {self.config.kafka_bootstrap_servers}\n"
            f"  - Kafka Topic: {self.config.kafka_topic}\n"
            f"  - DLQ Topic: {self.config.kafka_dlq_topic}\n"
            f"  - Batch Size: {self.config.batch_size}\n"
            f"  - Streaming Delay: {self.config.streaming_delay_seconds}s\n"
            f"  - Target EPS: {self.config.events_per_second}\n"
            f"  - Max Records: {self.config.max_records or 'All'}\n"
            f"  - Dry Run: {self.config.kafka_dry_run}"
        )

        reader = IncrementalCSVReader(str(resolved_path))
        producer = self._custom_producer or TransactionKafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topic=self.config.kafka_topic,
            dlq_topic=self.config.kafka_dlq_topic,
            dry_run=self.config.kafka_dry_run,
        )
        rate_limiter = StreamingRateLimiter(
            delay_seconds=self.config.streaming_delay_seconds,
            events_per_second=self.config.events_per_second,
        )

        self.start_time = time.time()
        last_log_time = self.start_time
        log_interval_records = 5000 if not self.config.streaming_delay_seconds else 500

        try:
            for batch in reader.stream_batches(
                batch_size=self.config.batch_size,
                max_records=self.config.max_records if self.config.max_records > 0 else None,
            ):
                if self._stop_requested:
                    logger.info("Halting batch processing due to shutdown request.")
                    break

                for row_num, raw_record in batch:
                    if self._stop_requested:
                        break

                    # Rate limiter pace
                    rate_limiter.pace()

                    # Validate record
                    val_result = validate_raw_record(raw_record, row_num=row_num)
                    self.total_processed += 1

                    if val_result.is_valid and val_result.record is not None:
                        self.valid_count += 1
                        if val_result.record.isFraud == 1:
                            self.fraud_count += 1
                        producer.publish_transaction(val_result.record)
                    else:
                        self.invalid_count += 1
                        if val_result.invalid_record is not None:
                            producer.publish_to_dlq(val_result.invalid_record)

                # Periodic progress metrics
                now = time.time()
                if (
                    self.total_processed % log_interval_records == 0
                    or (now - last_log_time) >= 10.0
                ):
                    elapsed = max(0.001, now - self.start_time)
                    rate = self.total_processed / elapsed
                    logger.info(
                        f"Progress: {self.total_processed:,} processed | "
                        f"Valid: {self.valid_count:,} | Invalid (DLQ): {self.invalid_count:,} | "
                        f"Fraud detected: {self.fraud_count:,} | Rate: {rate:.1f} rec/s"
                    )
                    last_log_time = now

            # Final flush of producer messages
            producer.flush()

        except KeyboardInterrupt:
            logger.warning("Pipeline interrupted by user (Ctrl+C).")
        except Exception as exc:
            logger.error(f"Error during pipeline execution: {exc}", exc_info=True)
            raise
        finally:
            self.end_time = time.time()
            # If we created the producer locally, ensure it is closed
            if self._custom_producer is None:
                producer.close()

        total_time = max(0.001, self.end_time - (self.start_time or self.end_time))
        avg_throughput = self.total_processed / total_time

        summary = {
            "dataset_path": str(resolved_path),
            "total_processed": self.total_processed,
            "valid_records": self.valid_count,
            "invalid_records": self.invalid_count,
            "fraud_records": self.fraud_count,
            "elapsed_seconds": round(total_time, 2),
            "records_per_second": round(avg_throughput, 1),
            "dry_run": self.config.kafka_dry_run,
        }

        logger.info(
            f"Pipeline Complete:\n"
            f"  - Total Processed: {self.total_processed:,}\n"
            f"  - Valid Records:   {self.valid_count:,}\n"
            f"  - Invalid Records: {self.invalid_count:,}\n"
            f"  - Fraud Records:   {self.fraud_count:,}\n"
            f"  - Duration:        {total_time:.2f}s\n"
            f"  - Average Rate:    {avg_throughput:.1f} rec/s"
        )

        return summary
