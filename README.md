# Financial Fraud Detection Platform — Member 1: Ingestion & Streaming Pipeline

This repository component implements the **Member 1 responsibilities** for the Financial Fraud Detection Platform:
1. **Memory-Efficient Python Batch Ingestion** for large PaySim datasets (~493 MB, 6.36M+ records)
2. **Apache Kafka Producer** with partition key routing, asynchronous callbacks, and Dead Letter Queue (DLQ) support
3. **Simulated Real-Time Streaming** with configurable playback speed and target throughput (Events Per Second / TPS)
4. **Strict Schema & Business Validation** using Pydantic v2
5. **Comprehensive Error Handling & Fault Tolerance**
6. **Configurable Structured Logging & Progress Telemetry**
7. **Automated Unit & Integration Test Suite** (using synthetic data)

---

## 1. Project Structure

```text
financial-fraud-platform/
├── dataset/
│   └── paysim.csv                  # Original PaySim dataset (~493 MB)
├── docker-compose.yml              # Local Apache Kafka (KRaft mode) + Kafka UI
├── requirements.txt                # Python project dependencies
├── .env.example                    # Environment configuration template
├── src/
│   ├── __init__.py
│   ├── config.py                   # Centralized configuration with env var resolution
│   ├── logger.py                   # Standardized structured logging setup
│   ├── models/
│   │   ├── __init__.py
│   │   └── transaction.py          # PaySimTransaction schema, validation, DLQ record
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── reader.py               # Memory-efficient incremental CSV batch reader
│   ├── producer/
│   │   ├── __init__.py
│   │   ├── kafka_producer.py       # Resilient Kafka producer with retry & callbacks
│   │   └── rate_limiter.py         # Real-time streaming rate limiter & pacer
│   └── pipeline.py                 # Ingestion pipeline coordinator
├── scripts/
│   ├── run_ingestion.py            # CLI entrypoint for batch & streaming ingestion
│   └── verify_consumer.py          # Message verification consumer script
└── tests/
    ├── __init__.py
    ├── conftest.py                 # Pytest synthetic fixtures
    ├── test_models.py              # Data model & validation tests
    ├── test_reader.py              # Incremental CSV reader tests
    ├── test_producer.py            # Kafka producer & mock tests
    └── test_pipeline.py            # End-to-end synthetic pipeline tests
```

---

## 2. Configuration & Environment Variables

The default dataset path is `dataset/paysim.csv`. You can customize it and all Kafka / streaming options using environment variables (or by placing them into a `.env` file):

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `DATASET_PATH` | `dataset/paysim.csv` | Path to the PaySim CSV dataset |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC` | `financial-transactions` | Kafka topic for valid transaction events |
| `KAFKA_DLQ_TOPIC` | `financial-transactions-dlq` | Dead Letter Queue topic for invalid/corrupted records |
| `BATCH_SIZE` | `5000` | Number of CSV rows read per batch chunk |
| `STREAMING_DELAY_SECONDS` | `0.0` | Delay between records for real-time simulation (e.g. `0.01` = ~100 msg/sec) |
| `EVENTS_PER_SECOND` | `0.0` | Target events per second (`0.0` = maximum throughput) |
| `MAX_RECORDS` | `0` | Limit number of records to ingest (`0` = process entire file) |
| `KAFKA_DRY_RUN` | `false` | If `true`, runs ingestion and validation without connecting to Kafka |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## 3. Installation & Setup

### Step 1: Install Python Dependencies
Ensure Python 3.10+ is installed:
```bash
pip install -r requirements.txt
```

### Step 2: (Optional) Set Environment Variables
Copy `.env.example` to `.env` if you wish to customize parameters:
```bash
cp .env.example .env
```

---

## 4. How to Start Apache Kafka

A pre-configured Docker Compose file (`docker-compose.yml`) is provided. It starts a single-node Apache Kafka cluster in modern KRaft mode (no Zookeeper required) along with **Kafka UI** for visual monitoring.

### Start Kafka with Docker Compose:
```bash
docker compose up -d
```

### Verify Kafka is running:
- **Broker Endpoint**: `localhost:9092`
- **Kafka Web UI**: Open your browser at [http://localhost:8080](http://localhost:8080)
- You can inspect active topics, partitions, consumer groups, and browse published messages in real time.

### Stop Kafka when finished:
```bash
docker compose down
```

---

## 5. How to Run Ingestion

The ingestion pipeline can be executed in three primary modes:

### Mode A: Dry-Run Mode (No Kafka Broker Required)
Test ingestion, incremental chunking, and validation without needing Kafka:
```bash
python scripts/run_ingestion.py --dry-run --max-records 1000
```

### Mode B: Full Batch Ingestion (Maximum Throughput)
Stream records at maximum network speed directly to Kafka:
```bash
python scripts/run_ingestion.py --max-records 50000
```
Or process the entire 6.36 million row dataset:
```bash
python scripts/run_ingestion.py
```

### Mode C: Simulated Real-Time Streaming
Simulate a live payment feed by pacing the producer:
- **At 100 events per second**:
  ```bash
  python scripts/run_ingestion.py --eps 100
  ```
- **With a 5 millisecond delay between events**:
  ```bash
  python scripts/run_ingestion.py --delay 0.005
  ```

### Mode D: Custom Dataset Path via CLI or Environment Variable
```bash
# Using CLI argument:
python scripts/run_ingestion.py --dataset-path path/to/custom_dataset.csv

# Using Environment Variable (PowerShell):
$env:DATASET_PATH="dataset/paysim.csv"
python scripts/run_ingestion.py --max-records 100
```

---

## 6. How to Verify Published Messages

You can verify that transactions are reaching the Kafka broker using any of the following methods:

### Method 1: Python Consumer Verification Script (Recommended)
We provide a standalone verification tool:
```bash
python scripts/verify_consumer.py --max-messages 10 --from-beginning
```
This script will output message key, partition, offset, and the formatted transaction JSON payload.

To inspect records in the Dead Letter Queue (DLQ):
```bash
python scripts/verify_consumer.py --topic financial-transactions-dlq --from-beginning
```

### Method 2: Web Browser (Kafka UI)
1. Open [http://localhost:8080](http://localhost:8080)
2. Navigate to **Topics** -> `financial-transactions` -> **Messages**
3. View transaction headers, payload values, and timestamp tracking.

### Method 3: Official Kafka Console Consumer (Docker)
```bash
docker exec -it fraud-platform-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic financial-transactions \
  --from-beginning \
  --max-messages 5
```

---

## 7. How to Run Automated Tests

The test suite runs entirely on **small synthetic data** fixtures and does not load the 493 MB dataset into memory, ensuring ultra-fast, regression-free execution.

Run the test suite using `pytest`:
```bash
pytest -v
```

### Test Coverage Summary:
- `tests/test_models.py`: Schema validation, type checking, negative value rejection, alias normalization (`oldbalanceOrg`/`oldbalanceOrig`), DLQ conversion.
- `tests/test_reader.py`: Memory-efficient generator streaming, batch chunking, `max_records` capping, missing file handling.
- `tests/test_producer.py`: Kafka producer dry-run, mock broker unit tests, async callbacks, message partition routing by `nameOrig`, rate limiting.
- `tests/test_pipeline.py`: End-to-end pipeline run on synthetic CSV, validation counts, fraud detection counts, graceful shutdown (`SIGINT`/`SIGTERM`).

---

## 8. Troubleshooting & Common Errors

### 1. `ConnectionError: Could not connect to Kafka broker at 'localhost:9092'`
- **Cause**: Kafka is not running or the port is unreachable.
- **Remedy**:
  1. Start Kafka via `docker compose up -d`.
  2. Verify that port `9092` is not blocked by another process: `netstat -ano | findstr :9092`.
  3. If testing without Kafka, pass the `--dry-run` flag: `python scripts/run_ingestion.py --dry-run`.

### 2. `FileNotFoundError: Dataset file not found at: '...'`
- **Cause**: The dataset CSV path does not exist.
- **Remedy**:
  1. Confirm `dataset/paysim.csv` exists in the project root.
  2. If located elsewhere, set the `DATASET_PATH` environment variable or pass `--dataset-path <path>`.

### 3. Out-Of-Memory (OOM) Concerns on 493 MB Dataset
- **Design Safeguard**: The reader uses Python's `csv.DictReader` generator pattern (`IncrementalCSVReader.stream_batches`), which streams rows line-by-line in configurable chunks (`BATCH_SIZE=5000`).
- Memory footprint remains constant (~30-50 MB) throughout the ingestion of millions of rows.

### 4. Topic Not Found or Leader Not Available
- **Remedy**: Kafka creates topics automatically by default (`auto.create.topics.enable=true`). If this is disabled in your cluster, manually create the topic:
  ```bash
  docker exec -it fraud-platform-kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic financial-transactions \
    --partitions 3 \
    --replication-factor 1
  ```

---

## 9. Downstream Consumer Contract (Member 2: Spark Structured Streaming)

This section specifies the interface contract between the **Member 1 Ingestion Producer** and the **Member 2 Spark Structured Streaming Consumer**:

### Kafka Stream Specifications
- **Primary Kafka Topic**: `financial-transactions`
- **Dead Letter Queue (DLQ) Topic**: `financial-transactions-dlq`
- **Message Partition Key**: `nameOrig` (UTF-8 string) — guarantees in-order arrival per originating customer account across partitions.
- **Value Format**: Valid JSON object encoded as UTF-8.

### JSON Payload Schema

| Field Name | PySpark `DataType` | PaySim Description | Example Value |
| :--- | :--- | :--- | :--- |
| `step` | `IntegerType()` | 1 step represents 1 hour of simulation | `1` |
| `type` | `StringType()` | Transaction type (`PAYMENT`, `TRANSFER`, `CASH_OUT`, `DEBIT`, `CASH_IN`) | `"PAYMENT"` |
| `amount` | `DoubleType()` | Transaction amount in currency units | `9839.64` |
| `nameOrig` | `StringType()` | Originating customer identifier | `"C1231006815"` |
| `oldbalanceOrg` | `DoubleType()` | Initial balance of origin account before transaction | `170136.0` |
| `newbalanceOrig` | `DoubleType()` | New balance of origin account after transaction | `160296.36` |
| `nameDest` | `StringType()` | Recipient account identifier (`M...` for merchant, `C...` for customer) | `"M1979787155"` |
| `oldbalanceDest` | `DoubleType()` | Initial balance of recipient before transaction | `0.0` |
| `newbalanceDest` | `DoubleType()` | New balance of recipient after transaction | `0.0` |
| `isFraud` | `IntegerType()` | Ground truth fraud label (`1` = Fraud, `0` = Normal) — *Target label only* | `0` |
| `isFlaggedFraud` | `IntegerType()` | Rule-based fraud flag (`1` = Flagged, `0` = Normal) — *Audit flag only* | `0` |
| `ingestion_timestamp` | `DoubleType()` | Epoch timestamp (seconds) when ingested into pipeline | `1792160481.93` |

> [!IMPORTANT]
> **Data Leakage Safeguard (Live ML Feature Separation)**:
> The fields `isFraud` and `isFlaggedFraud` are emitted exclusively as **ground-truth evaluation targets** and regulatory audit flags. In downstream consumer processing (Member 2 Spark feature engineering and Member 3 model inference), these labels **must never be used as live prediction features** (which would introduce target leakage). Live ML feature sets must be derived strictly from transaction attributes (`type`, `amount`, `nameOrig`, `nameDest`, balances) and rolling temporal aggregation state.

### Member 2 PySpark Structured Streaming Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

spark = (
    SparkSession.builder.appName("FraudDetectionStreamingConsumer")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

# 1. Define schema matching Member 1's JSON contract
transaction_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("ingestion_timestamp", DoubleType(), True),
])

# 2. Read from Kafka stream
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "financial-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Parse JSON payload and partition key
parsed_df = (
    kafka_df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as json_payload")
    .withColumn("data", from_json(col("json_payload"), transaction_schema))
    .select("key", "data.*")
)

# 4. Stream to console / processing sink
query = (
    parsed_df.writeStream.format("console")
    .outputMode("append")
    .start()
)
query.awaitTermination()
```

---

## 10. Member 2: Data Engineering & Lakehouse Implementation

Member 2 integrates directly with Member 1's outputs, implementing an **Apache Spark Medallion Lakehouse** and modular ETL/ELT pipelines:

```text
data/
├── bronze/transactions/         # Raw Parquet archive + ingestion audit metadata
├── silver/transactions/         # Cleaned, schema-conformed & deduplicated Parquet
├── quarantine/transactions/     # Rejected records with quarantine_reason & timestamp
└── gold/
    ├── enriched_transactions/   # Primary ML feature store for Member 3
    ├── customer_aggregates/     # Account-level behavioral summaries
    └── hourly_aggregates/       # Time-series volume and fraud metrics
```

### Quick Start for Member 2 (Batch ETL)
```bash
# Run batch ETL on sample dataset:
python scripts/run_lakehouse_etl.py --mode batch --input-path dataset/sample_paysim.csv

# Run batch ETL on full dataset (capped at 50,000 records):
python scripts/run_lakehouse_etl.py --mode batch --input-path dataset/paysim.csv --max-records 50000
```

### Quick Start for Member 2 (Kafka Streaming ETL)
```bash
# Read Kafka topic 'financial-transactions' and stream micro-batches into Bronze & Silver:
python scripts/run_lakehouse_etl.py --mode streaming
```

### Running the Full Test Suite
```bash
pytest -v
```
Runs 39 tests (Member 1 ingestion + Member 2 Lakehouse & ETL).

For full details, data dictionaries, feature definitions, and copy-paste PySpark snippets for **Member 3 (ML)**, see [`docs/MEMBER2_DATA_ENGINEERING.md`](docs/MEMBER2_DATA_ENGINEERING.md).


