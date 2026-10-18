import pytest
from pathlib import Path
from src.ingestion.reader import (
    IncrementalCSVReader,
    stream_dataset_records,
    stream_dataset_batches,
)


def test_reader_file_not_found():
    with pytest.raises(FileNotFoundError) as exc_info:
        IncrementalCSVReader("non_existent_directory/fake_paysim.csv")
    assert "Dataset file not found" in str(exc_info.value)


def test_stream_records(synthetic_csv_file: Path):
    reader = IncrementalCSVReader(str(synthetic_csv_file))
    records = list(reader.stream_records())
    assert len(records) == 10
    
    # Check row indices and columns of the first record
    row_num, first_row = records[0]
    assert row_num == 1
    assert first_row["type"] == "PAYMENT"
    assert first_row["nameOrig"] == "C1000000001"


def test_stream_records_max_records(synthetic_csv_file: Path):
    reader = IncrementalCSVReader(str(synthetic_csv_file))
    records = list(reader.stream_records(max_records=3))
    assert len(records) == 3


def test_stream_batches(synthetic_csv_file: Path):
    reader = IncrementalCSVReader(str(synthetic_csv_file))
    # Batch size 4 with 10 records -> 3 batches (4, 4, 2)
    batches = list(reader.stream_batches(batch_size=4))
    assert len(batches) == 3
    assert len(batches[0]) == 4
    assert len(batches[1]) == 4
    assert len(batches[2]) == 2


def test_stream_batches_max_records(synthetic_csv_file: Path):
    reader = IncrementalCSVReader(str(synthetic_csv_file))
    batches = list(reader.stream_batches(batch_size=3, max_records=5))
    total_records = sum(len(b) for b in batches)
    assert total_records == 5


def test_convenience_helpers(synthetic_csv_file: Path):
    records = list(stream_dataset_records(str(synthetic_csv_file), max_records=2))
    assert len(records) == 2

    batches = list(stream_dataset_batches(str(synthetic_csv_file), batch_size=5))
    assert len(batches) == 2
