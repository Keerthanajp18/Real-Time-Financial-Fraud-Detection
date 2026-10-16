import json
import time
from unittest.mock import MagicMock, patch
import pytest
from src.models.transaction import (
    PaySimTransaction,
    InvalidTransactionRecord,
    TransactionType,
)
from src.producer.kafka_producer import TransactionKafkaProducer
from src.producer.rate_limiter import StreamingRateLimiter


def test_producer_dry_run_publish(sample_valid_dict):
    producer = TransactionKafkaProducer(
        bootstrap_servers="localhost:9092",
        topic="test-topic",
        dlq_topic="test-dlq",
        dry_run=True,
    )
    tx = PaySimTransaction(**sample_valid_dict)
    success = producer.publish_transaction(tx)

    assert success is True
    assert producer.messages_sent == 1
    assert producer.messages_acknowledged == 1
    assert producer.messages_failed == 0

    producer.close()


def test_producer_dry_run_dlq():
    producer = TransactionKafkaProducer(dry_run=True)
    inv = InvalidTransactionRecord(
        raw_data={"broken": "data"},
        error_message="Invalid row structure",
        row_number=10,
    )
    success = producer.publish_to_dlq(inv)
    assert success is True
    assert producer.dlq_messages_sent == 1

    producer.close()


@patch("kafka.KafkaProducer")
def test_producer_with_mocked_kafka(mock_kafka_cls, sample_valid_dict):
    mock_instance = MagicMock()
    mock_kafka_cls.return_value = mock_instance

    producer = TransactionKafkaProducer(
        bootstrap_servers="mock-broker:9092",
        topic="financial-transactions",
        dry_run=False,
    )

    tx = PaySimTransaction(**sample_valid_dict)
    success = producer.publish_transaction(tx)

    assert success is True
    assert producer.messages_sent == 1

    # Verify send was called on mock with expected key and topic
    mock_instance.send.assert_called_once()
    call_args, call_kwargs = mock_instance.send.call_args
    assert call_args[0] == "financial-transactions"
    assert call_kwargs["key"] == tx.nameOrig
    assert call_kwargs["value"]["amount"] == tx.amount

    producer.close()
    mock_instance.flush.assert_called()
    mock_instance.close.assert_called()


def test_streaming_rate_limiter():
    # Limiter set to 50 events per second (approx 0.02s interval)
    limiter = StreamingRateLimiter(events_per_second=50.0)
    start = time.perf_counter()
    for _ in range(3):
        limiter.pace()
    duration = time.perf_counter() - start
    # Should take at least ~0.03 seconds for 2 paced gaps
    assert duration >= 0.02
