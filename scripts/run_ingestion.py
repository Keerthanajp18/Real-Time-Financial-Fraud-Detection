import argparse
import signal
import sys
from pathlib import Path

# Add project root to sys.path so scripts can run standalone
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.config import get_config
from src.logger import setup_logger
from src.pipeline import IngestionPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PaySim Financial Fraud Platform - Ingestion & Streaming Pipeline (Member 1)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--dataset-path",
        type=str,
        default=None,
        help="Path to paysim.csv dataset file (defaults to DATASET_PATH env or dataset/paysim.csv)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap server(s) (e.g. localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Kafka topic for valid transactions",
    )
    parser.add_argument(
        "--dlq-topic",
        type=str,
        default=None,
        help="Kafka topic for invalid transactions (DLQ)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Chunk size for reading CSV rows incrementally",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Streaming delay in seconds between events for real-time simulation",
    )
    parser.add_argument(
        "--eps",
        type=float,
        default=None,
        help="Target streaming throughput in events per second",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum records to process (0 for entire dataset)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run ingestion and validation without connecting to Kafka broker",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Set logging level",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Build config overrides from CLI arguments
    overrides = {}
    if args.dataset_path is not None:
        overrides["dataset_path"] = args.dataset_path
    if args.bootstrap_servers is not None:
        overrides["kafka_bootstrap_servers"] = args.bootstrap_servers
    if args.topic is not None:
        overrides["kafka_topic"] = args.topic
    if args.dlq_topic is not None:
        overrides["kafka_dlq_topic"] = args.dlq_topic
    if args.batch_size is not None:
        overrides["batch_size"] = args.batch_size
    if args.delay is not None:
        overrides["streaming_delay_seconds"] = args.delay
    if args.eps is not None:
        overrides["events_per_second"] = args.eps
    if args.max_records is not None:
        overrides["max_records"] = args.max_records
    if args.dry_run:
        overrides["kafka_dry_run"] = True
    if args.log_level is not None:
        overrides["log_level"] = args.log_level

    config = get_config(**overrides)
    logger = setup_logger("run_ingestion", log_level=config.log_level)

    pipeline = IngestionPipeline(config=config)

    # Attach graceful shutdown handler
    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}. Requesting pipeline graceful stop...")
        pipeline.request_stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        summary = pipeline.run()
        print("\n" + "=" * 60)
        print("INGESTION RUN SUMMARY")
        print("=" * 60)
        for key, value in summary.items():
            print(f"  {key:<22}: {value}")
        print("=" * 60 + "\n")
        return 0
    except KeyboardInterrupt:
        logger.info("Process terminated by user.")
        return 0
    except Exception as e:
        logger.error(f"Fatal error during execution: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
