import json
import time
from typing import Any, Callable, Dict, Optional
from src.logger import setup_logger
from src.models.transaction import PaySimTransaction, InvalidTransactionRecord

logger = setup_logger("kafka_producer")


class TransactionKafkaProducer:
    """
    Robust Apache Kafka producer wrapper for financial fraud transactions.
    Supports asynchronous publishing with callbacks, partition key routing,
    dead letter queue routing, and dry-run mode.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "financial-transactions",
        dlq_topic: str = "financial-transactions-dlq",
        acks: str = "all",
        retries: int = 3,
        retry_backoff_ms: int = 500,
        dry_run: bool = False,
        client_id: str = "fraud-ingestion-producer",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.dry_run = dry_run
        self.client_id = client_id

        # Internal metric counters
        self.messages_sent = 0
        self.messages_acknowledged = 0
        self.messages_failed = 0
        self.dlq_messages_sent = 0

        self._producer = None

        if not self.dry_run:
            self._init_producer(
                bootstrap_servers=bootstrap_servers,
                acks=acks,
                retries=retries,
                retry_backoff_ms=retry_backoff_ms,
            )
        else:
            logger.info("TransactionKafkaProducer initialized in DRY-RUN mode. No broker connection will be made.")

    def _init_producer(
        self,
        bootstrap_servers: str,
        acks: str,
        retries: int,
        retry_backoff_ms: int,
    ) -> None:
        try:
            from kafka import KafkaProducer

            logger.info(f"Connecting to Kafka brokers at: {bootstrap_servers}...")
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                client_id=self.client_id,
                acks=acks,
                retries=retries,
                retry_backoff_ms=retry_backoff_ms,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_block_ms=10000,  # 10s timeout when broker is unreachable
            )
            logger.info(f"Successfully connected to Kafka brokers at: {bootstrap_servers}")
        except Exception as e:
            logger.error(
                f"Failed to initialize Kafka producer with broker '{bootstrap_servers}': {e}. "
                f"Tip: If Kafka is not running, start it using docker-compose or run with --dry-run."
            )
            raise ConnectionError(
                f"Could not connect to Kafka broker at '{bootstrap_servers}': {e}"
            ) from e

    def _on_send_success(self, record_metadata: Any, key: str) -> None:
        """Callback invoked when broker acknowledges delivery."""
        self.messages_acknowledged += 1
        logger.debug(
            f"Delivered message [key={key}] -> topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, offset={record_metadata.offset}"
        )

    def _on_send_error(self, exc: Exception, key: str, payload: Dict[str, Any]) -> None:
        """Callback invoked when broker rejects delivery or times out."""
        self.messages_failed += 1
        logger.error(f"Failed to deliver message [key={key}]: {exc}")

    def publish_transaction(
        self,
        transaction: PaySimTransaction,
        custom_key: Optional[str] = None,
        callback: Optional[Callable] = None,
    ) -> bool:
        """
        Publishes a validated PaySimTransaction to the Kafka topic.
        Uses origin account ID (nameOrig) as partition key to ensure per-account ordering.
        """
        key = custom_key or transaction.get_partition_key()
        payload = transaction.to_kafka_dict()

        self.messages_sent += 1

        if self.dry_run:
            self.messages_acknowledged += 1
            logger.debug(f"[DRY-RUN] Published to {self.topic} [key={key}]")
            return True

        if self._producer is None:
            raise RuntimeError("Kafka producer instance is not initialized.")

        try:
            future = self._producer.send(
                self.topic,
                key=key,
                value=payload,
            )
            future.add_callback(lambda meta: self._on_send_success(meta, key))
            future.add_errback(lambda exc: self._on_send_error(exc, key, payload))
            if callback:
                future.add_callback(callback)
            return True
        except Exception as exc:
            self.messages_failed += 1
            logger.error(f"Synchronous error sending transaction [key={key}]: {exc}")
            return False

    def publish_to_dlq(
        self,
        invalid_record: InvalidTransactionRecord,
    ) -> bool:
        """
        Publishes an invalid transaction record to the Dead Letter Queue (DLQ) topic.
        """
        key = str(invalid_record.row_number or "unknown_row")
        payload = invalid_record.model_dump()

        self.dlq_messages_sent += 1

        if self.dry_run:
            logger.debug(f"[DRY-RUN] Sent to DLQ topic {self.dlq_topic} [key={key}]: {invalid_record.error_message}")
            return True

        if self._producer is None:
            raise RuntimeError("Kafka producer instance is not initialized.")

        try:
            future = self._producer.send(
                self.dlq_topic,
                key=key,
                value=payload,
            )
            future.add_errback(lambda exc: logger.error(f"Failed to publish to DLQ [key={key}]: {exc}"))
            return True
        except Exception as exc:
            logger.error(f"Failed to send record to DLQ topic: {exc}")
            return False

    def flush(self, timeout: Optional[float] = None) -> None:
        """Flushes in-flight producer messages."""
        if not self.dry_run and self._producer is not None:
            logger.debug("Flushing Kafka producer buffer...")
            self._producer.flush(timeout=timeout)

    def close(self, timeout: Optional[float] = 10.0) -> None:
        """Gracefully flushes and closes the Kafka producer."""
        if not self.dry_run and self._producer is not None:
            logger.info("Closing Kafka producer connection...")
            self._producer.flush(timeout=timeout)
            self._producer.close(timeout=timeout)
            self._producer = None
        logger.info(
            f"Producer statistics: sent={self.messages_sent}, "
            f"acked={self.messages_acknowledged}, failed={self.messages_failed}, "
            f"dlq={self.dlq_messages_sent}"
        )
