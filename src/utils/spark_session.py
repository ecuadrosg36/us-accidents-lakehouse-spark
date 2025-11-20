"""
Spark Session Builder with optimizations.
"""

from pyspark.sql import SparkSession
from src.config.config_loader import get_config
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def create_spark_session(app_name: str = None) -> SparkSession:
    """
    Create optimized Spark session.

    Features:
    - Adaptive Query Execution (AQE)
    - Dynamic partition tuning
    - Optimized shuffle settings

    Args:
        app_name: Optional application name

    Returns:
        Configured SparkSession
    """
    config = get_config()

    if app_name is None:
        app_name = config.spark_app_name

    logger.info(f"Creating Spark session: {app_name}")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", str(config.enable_aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
        .getOrCreate()
    )

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark session created successfully")
    logger.info(f"AQE Enabled: {config.enable_aqe}")
    logger.info(f"Shuffle Partitions: {config.shuffle_partitions}")

    return spark
