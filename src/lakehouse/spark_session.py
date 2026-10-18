import os
import sys
import platform
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession
from src.logger import setup_logger

logger = setup_logger("spark_session")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _configure_windows_environment() -> None:
    """
    Configures environment variables required for PySpark execution on Windows.
    Sets JAVA_HOME, HADOOP_HOME (for winutils.exe), and binds PySpark python executables.
    """
    # 1. Ensure Python worker paths match current virtualenv
    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable
    if "PYSPARK_DRIVER_PYTHON" not in os.environ:
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # 2. Resolve JAVA_HOME if not already set
    if not os.environ.get("JAVA_HOME"):
        common_java_paths = [
            Path("C:/Program Files/Eclipse Adoptium/jdk-17.0.20.101-hotspot"),
            Path("C:/Program Files/Eclipse Adoptium"),
            Path("C:/Program Files/Java"),
            Path("C:/Program Files/Amazon Corretto"),
        ]
        for base in common_java_paths:
            if base.exists():
                if (base / "bin" / "java.exe").exists():
                    os.environ["JAVA_HOME"] = str(base)
                    logger.info(f"Auto-configured JAVA_HOME to: {base}")
                    break
                # Check subdirectories
                for sub in base.glob("jdk*"):
                    if (sub / "bin" / "java.exe").exists():
                        os.environ["JAVA_HOME"] = str(sub)
                        logger.info(f"Auto-configured JAVA_HOME to: {sub}")
                        break
            if os.environ.get("JAVA_HOME"):
                break

    # 3. Resolve HADOOP_HOME on Windows for winutils.exe
    if platform.system() == "Windows" and not os.environ.get("HADOOP_HOME"):
        local_hadoop = PROJECT_ROOT / "hadoop"
        if (local_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(local_hadoop)
            bin_path = str(local_hadoop / "bin")
            if bin_path not in os.environ.get("PATH", ""):
                os.environ["PATH"] = f"{bin_path};{os.environ.get('PATH', '')}"
            logger.info(f"Auto-configured HADOOP_HOME to: {local_hadoop}")


def get_spark_session(
    app_name: str = "FraudDetectionLakehouse",
    master: str = "local[*]",
    enable_kafka: bool = False,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    """
    Creates or retrieves an optimized, resilient Apache Spark session for local/cluster lakehouse workloads.
    
    Args:
        app_name: Name of the Spark application.
        master: Master URL (default: 'local[*]').
        enable_kafka: If True, adds the Kafka structured streaming JAR package.
        extra_configs: Optional dictionary of extra Spark configuration keys and values.
        
    Returns:
        Configured SparkSession instance.
    """
    _configure_windows_environment()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Windows network binding to prevent worker timeout
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Timezone and SQL standards
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Memory and partitions tuning for single-node development
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.enabled", "false")  # Disable web UI for faster test execution
    )

    if enable_kafka:
        # Include Kafka Structured Streaming connector package
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.debug(f"SparkSession created: app_name={app_name}, version={spark.version}")
    return spark


def stop_spark_session(spark: Optional[SparkSession] = None) -> None:
    """Safely stops active Spark session."""
    if spark is not None:
        try:
            spark.stop()
            logger.debug("SparkSession stopped successfully.")
        except Exception as exc:
            logger.warning(f"Error while stopping SparkSession: {exc}")
