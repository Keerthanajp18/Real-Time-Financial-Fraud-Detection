"""
CLI Entry Point for Lakehouse ETL (Member 2: Data Engineering & Lakehouse).
Supports running batch ETL (CSV/JSON/Parquet -> Bronze -> Silver -> Gold)
and real-time streaming ETL from Kafka.
"""
import argparse
import signal
import sys
from pathlib import Path

# Add project root to sys.path so scripts can run standalone
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.etl.batch_etl import BatchLakehouseETL
from src.etl.streaming_etl import StreamingLakehouseETL
from src.lakehouse.config import get_lakehouse_config
from src.lakehouse.spark_session import stop_spark_session
from src.logger import setup_logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Financial Fraud Platform - Lakehouse ETL Pipeline (Member 2: Spark & Lakehouse)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["batch", "streaming"],
        default="batch",
        help="ETL execution mode: 'batch' (process files) or 'streaming' (consume Kafka stream)",
    )
    parser.add_argument(
        "--input-path",
        type=str,
        default=None,
        help="Path to input dataset file or directory (defaults to DATASET_PATH or dataset/paysim.csv)",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "json", "parquet"],
        default="csv",
        help="Format of input batch data",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Limit number of records to process in batch mode (for testing/sample runs)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Base directory for Lakehouse storage (bronze, silver, gold, quarantine)",
    )
    parser.add_argument(
        "--bronze-mode",
        type=str,
        choices=["append", "overwrite"],
        default="append",
        help="Bronze layer save mode",
    )
    parser.add_argument(
        "--silver-mode",
        type=str,
        choices=["append", "overwrite"],
        default="append",
        help="Silver layer save mode",
    )
    parser.add_argument(
        "--gold-mode",
        type=str,
        choices=["overwrite", "append"],
        default="overwrite",
        help="Gold layer save mode",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap server(s) for streaming mode",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Kafka topic to consume for streaming mode",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity level",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logger = setup_logger("run_lakehouse_etl", log_level=args.log_level)

    # Build config overrides
    overrides = {}
    if args.data_dir:
        overrides["data_dir"] = args.data_dir
        overrides["bronze_dir"] = str(Path(args.data_dir) / "bronze" / "transactions")
        overrides["silver_dir"] = str(Path(args.data_dir) / "silver" / "transactions")
        overrides["quarantine_dir"] = str(Path(args.data_dir) / "quarantine" / "transactions")
        overrides["gold_dir"] = str(Path(args.data_dir) / "gold")
        overrides["checkpoint_dir"] = str(Path(args.data_dir) / "checkpoints")

    if args.input_path:
        overrides["default_input_dataset"] = args.input_path
    if args.bootstrap_servers:
        overrides["kafka_bootstrap_servers"] = args.bootstrap_servers
    if args.topic:
        overrides["kafka_topic"] = args.topic

    config = get_lakehouse_config(**overrides)

    if args.mode == "batch":
        logger.info("Initializing Batch Lakehouse ETL Pipeline...")
        etl = BatchLakehouseETL(config=config)

        try:
            summary = etl.run(
                input_path=args.input_path,
                file_format=args.format,
                bronze_mode=args.bronze_mode,
                silver_mode=args.silver_mode,
                gold_mode=args.gold_mode,
                max_records=args.max_records,
            )
            return 0
        except Exception as exc:
            logger.error(f"Batch ETL failed: {exc}", exc_info=True)
            return 1
        finally:
            stop_spark_session(etl.spark)

    elif args.mode == "streaming":
        logger.info("Initializing Streaming Lakehouse ETL Pipeline...")
        streaming_etl = StreamingLakehouseETL(config=config)

        query = None

        def handle_signal(sig, frame):
            logger.info("Termination signal received. Stopping streaming query gracefully...")
            if query and query.isActive:
                query.stop()
            stop_spark_session(streaming_etl.spark)
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            query = streaming_etl.start_stream()
            logger.info("Streaming query is running. Press Ctrl+C to stop.")
            query.awaitTermination()
            return 0
        except Exception as exc:
            logger.error(f"Streaming ETL failed: {exc}", exc_info=True)
            return 1
        finally:
            if query and query.isActive:
                query.stop()
            stop_spark_session(streaming_etl.spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
