"""
Lakehouse module for Member 2: Data Engineering & Lakehouse.
Provides Spark session management, Lakehouse layers (Bronze, Silver, Gold), and storage configs.
"""
from src.lakehouse.bronze import BronzeLayerManager
from src.lakehouse.config import LakehouseConfig, get_lakehouse_config
from src.lakehouse.gold import GoldLayerManager
from src.lakehouse.silver import SilverLayerManager
from src.lakehouse.spark_session import get_spark_session, stop_spark_session

__all__ = [
    "LakehouseConfig",
    "get_lakehouse_config",
    "BronzeLayerManager",
    "SilverLayerManager",
    "GoldLayerManager",
    "get_spark_session",
    "stop_spark_session",
]
