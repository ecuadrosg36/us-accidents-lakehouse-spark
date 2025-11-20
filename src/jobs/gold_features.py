"""
Gold Layer Transformation.

Create ML-ready features and optimize file layout.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from src.config.config_loader import get_config
from src.utils.feature_engineering import FeatureEngineer
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def transform_silver_to_gold(spark: SparkSession) -> None:
    """
    Transform Silver to Gold layer.
    
    Steps:
    1. Read Silver Parquet
    2. Apply feature engineering
    3. Select ML-ready columns
    4. Optimize file sizes
    5. Write to Gold
    
    Args:
        spark: SparkSession instance
    """
    config = get_config()
    fe = FeatureEngineer()
    
    logger.info(f"Reading Silver data from: {config.silver_path}")
    
    # Read Silver
    df = spark.read.parquet(config.silver_path)
    logger.info(f"Silver records: {df.count()}")
    
    # Apply feature engineering
    logger.info("Creating features...")
    
    df = fe.create_temporal_features(df)
    df = fe.create_location_features(df)
    df = fe.create_weather_features(df)
    df = fe.create_infrastructure_features(df)
    
    # Select ML features
    df = fe.select_ml_features(df)
    
    # Add processing timestamp
    df = df.withColumn("gold_processed_at", current_timestamp())
    
    logger.info(f"Final feature count: {len(df.columns)}")
    
    # Optimize partitions for 128-256MB file sizes
    # Estimate: ~500K rows per partition for target file size
    target_rows_per_partition = 500000
    num_partitions = max(1, int(df.count() / target_rows_per_partition))
    
    logger.info(f"Optimizing to {num_partitions} partitions")
    df = df.repartition(num_partitions)
    
    # Write to Gold
    logger.info(f"Writing to Gold layer: {config.gold_path}")
    
    df.write \
        .mode("overwrite") \
        .parquet(config.gold_path)
    
    logger.info("Gold transformation complete!")
    logger.info(f"Gold records: {df.count()}")


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session
    
    spark = create_spark_session("Gold-Features")
    transform_silver_to_gold(spark)
    spark.stop()
