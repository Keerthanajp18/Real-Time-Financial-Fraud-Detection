import argparse
import json
import sys
from pathlib import Path

# Add project root to sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.logger import setup_logger

logger = setup_logger("verify_consumer")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify and inspect published messages from Kafka topic",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap server(s)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="financial-transactions",
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Maximum messages to consume before stopping",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=10.0,
        help="Timeout in seconds when waiting for messages",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        default=False,
        help="Consume messages starting from the earliest offset",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.error("kafka-python package is required to run the consumer verification script.")
        return 1

    auto_offset = "earliest" if args.from_beginning else "latest"
    logger.info(
        f"Connecting consumer to {args.bootstrap_servers} on topic '{args.topic}' "
        f"(offset={auto_offset}, max_messages={args.max_messages})..."
    )

    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.bootstrap_servers.split(","),
            auto_offset_reset=auto_offset,
            enable_auto_commit=False,
            consumer_timeout_ms=int(args.timeout_sec * 1000),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
            group_id="fraud-platform-verifier",
        )
    except Exception as exc:
        logger.error(f"Failed to connect Kafka consumer: {exc}")
        return 1

    count = 0
    print("\n" + "=" * 70)
    print(f"VERIFYING KAFKA MESSAGES: {args.topic}")
    print("=" * 70)

    try:
        for message in consumer:
            count += 1
            print(
                f"\n[Message #{count}] Partition={message.partition} | Offset={message.offset} | "
                f"Key={message.key}"
            )
            print("Payload:")
            print(json.dumps(message.value, indent=2))

            if count >= args.max_messages:
                break

        if count == 0:
            logger.warning(
                f"No messages received within {args.timeout_sec}s timeout. "
                f"Check if the producer has sent data to '{args.topic}' or run with --from-beginning."
            )
        else:
            logger.info(f"Successfully received and verified {count} messages from topic '{args.topic}'.")

    except KeyboardInterrupt:
        logger.info("Verification interrupted by user.")
    finally:
        consumer.close()

    print("\n" + "=" * 70 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
