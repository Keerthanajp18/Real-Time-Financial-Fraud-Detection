"""
Streaming Lakehouse ETL: Spark Structured Streaming from Kafka into Lakehouse layers.
Reads continuous transactions from Kafka topic 'financial-transactions',
parses JSON payloads, and persists micro-batches into Bronze and Silver layers.
"""
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.lakehouse.bronze import BronzeLayerManager
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.silver import SilverLayerManager
from src.lakehouse.spark_session import get_spark_session
from src.logger import setup_logger

logger = setup_logger("streaming_etl")

# Schema contract matching Member 1 Kafka producer
PAYMENT_STREAM_SCHEMA = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("oldbalanceOrig", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("ingestion_timestamp", DoubleType(), True),
])


class StreamingLakehouseETL:
    """
    Spark Structured Streaming pipeline connecting Member 1's Kafka output
    directly to the Lakehouse layers.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[LakehouseConfig] = None,
    ):
        self.config = config or get_lakehouse_config()
        self.spark = spark or get_spark_session(
            app_name=f"{self.config.spark_app_name}_Streaming",
            master=self.config.spark_master,
            enable_kafka=True,
        )
        self.bronze_manager = BronzeLayerManager(self.config)
        self.silver_manager = SilverLayerManager(self.config)

    def _process_micro_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Micro-batch callback function executed by Spark Structured Streaming.
        Atomically lands raw batch into Bronze and cleaned data into Silver.
        """
        if batch_df.rdd.isEmpty():
            logger.debug(f"[Micro-Batch #{batch_id}] Empty batch received. Skipping.")
            return

        batch_count = batch_df.count()
        logger.info(f"[Micro-Batch #{batch_id}] Processing {batch_count:,} incoming streaming transactions...")

        # 1. Append raw micro-batch to Bronze
        self.bronze_manager.write_bronze(
            df=batch_df,
            source_name=f"kafka://{self.config.kafka_topic}",
            mode="append",
            batch_id=f"streaming_batch_{batch_id}",
        )

        # 2. Process through Silver (validation, quarantine, deduplication)
        self.silver_manager.process_and_write(
            df=batch_df,
            mode="append",
        )

        logger.info(f"[Micro-Batch #{batch_id}] Successfully ingested into Bronze and Silver layers.")

    def start_stream(
        self,
        trigger_processing_time: str = "5 seconds",
    ) -> StreamingQuery:
        """
        Initializes and starts the Spark Structured Streaming query.
        
        Args:
            trigger_processing_time: Micro-batch trigger interval (e.g. '5 seconds').
            
        Returns:
            Active StreamingQuery handle.
        """
        logger.info(
            f"Connecting Spark Structured Streaming to Kafka:\n"
            f"  - Brokers:    {self.config.kafka_bootstrap_servers}\n"
            f"  - Topic:      {self.config.kafka_topic}\n"
            f"  - Offsets:    {self.config.kafka_starting_offsets}\n"
            f"  - Checkpoint: {self.config.checkpoint_dir}"
        )

        # 1. Read Kafka stream
        raw_kafka_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("subscribe", self.config.kafka_topic)
            .option("startingOffsets", self.config.kafka_starting_offsets)
            .load()
        )

        # 2. Parse JSON payload matching Member 1 contract
        parsed_df = (
            raw_kafka_df.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_payload")
            .withColumn("data", F.from_json(F.col("json_payload"), PAYMENT_STREAM_SCHEMA))
            .select("data.*")
        )

        # 3. Micro-batch sink via foreachBatch
        query = (
            parsed_df.writeStream.trigger(processingTime=trigger_processing_time)
            .foreachBatch(self._process_micro_batch)
            .option("checkpointLocation", self.config.checkpoint_dir)
            .start()
        )

        logger.info(f"Structured Streaming query active (ID: {query.id}, RunID: {query.runId})")
        return query
