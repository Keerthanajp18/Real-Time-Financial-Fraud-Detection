import csv
import os
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from src.logger import setup_logger

logger = setup_logger("ingestion_reader")


class IncrementalCSVReader:
    """
    Memory-efficient CSV reader that processes datasets incrementally
    without loading the entire file into RAM.
    """

    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self._validate_file_path()

    def _validate_file_path(self) -> None:
        if not self.file_path.exists():
            raise FileNotFoundError(
                f"Dataset file not found at: '{self.file_path}'. "
                f"Ensure the dataset path is valid or configure the DATASET_PATH environment variable."
            )
        if not self.file_path.is_file():
            raise IsADirectoryError(
                f"Specified dataset path is a directory, not a file: '{self.file_path}'"
            )
        file_size_mb = self.file_path.stat().st_size / (1024 * 1024)
        logger.info(
            f"Initialized IncrementalCSVReader for: {self.file_path.name} "
            f"({file_size_mb:.2f} MB)"
        )

    def stream_records(
        self,
        max_records: Optional[int] = None,
        skip_rows: int = 0,
    ) -> Generator[Tuple[int, Dict[str, Any]], None, None]:
        """
        Streams records one by one as a generator.
        Yields (row_number, row_dict).
        
        Args:
            max_records: Maximum rows to yield (None or <= 0 for all).
            skip_rows: Number of data rows to skip from the start.
        """
        yielded_count = 0
        current_row_idx = 0

        with open(self.file_path, mode="r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError(f"CSV file '{self.file_path}' appears to be empty or missing headers.")

            # Clean header keys (strip whitespace/BOM)
            reader.fieldnames = [k.strip() if k else k for k in reader.fieldnames]

            for raw_row in reader:
                current_row_idx += 1

                if current_row_idx <= skip_rows:
                    continue

                yield (current_row_idx, raw_row)
                yielded_count += 1

                if max_records and max_records > 0 and yielded_count >= max_records:
                    logger.info(f"Reached max_records limit ({max_records}). Stopping stream.")
                    break

    def stream_batches(
        self,
        batch_size: int = 5000,
        max_records: Optional[int] = None,
    ) -> Generator[List[Tuple[int, Dict[str, Any]]], None, None]:
        """
        Streams records in chunks/batches.
        Yields a list of (row_number, row_dict) tuples.
        
        Args:
            batch_size: Number of records per chunk.
            max_records: Maximum total records to yield (None or <= 0 for all).
        """
        batch: List[Tuple[int, Dict[str, Any]]] = []
        total_yielded = 0

        for row_idx, record in self.stream_records(max_records=max_records):
            batch.append((row_idx, record))
            if len(batch) >= batch_size:
                yield batch
                total_yielded += len(batch)
                batch = []

        if batch:
            yield batch
            total_yielded += len(batch)


def stream_dataset_records(
    file_path: str,
    max_records: Optional[int] = None,
) -> Generator[Tuple[int, Dict[str, Any]], None, None]:
    """Helper shortcut to stream individual records."""
    reader = IncrementalCSVReader(file_path)
    yield from reader.stream_records(max_records=max_records)


def stream_dataset_batches(
    file_path: str,
    batch_size: int = 5000,
    max_records: Optional[int] = None,
) -> Generator[List[Tuple[int, Dict[str, Any]]], None, None]:
    """Helper shortcut to stream batches."""
    reader = IncrementalCSVReader(file_path)
    yield from reader.stream_batches(batch_size=batch_size, max_records=max_records)
