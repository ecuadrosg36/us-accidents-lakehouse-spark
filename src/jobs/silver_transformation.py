"""
Silver Layer Transformation.

Clean, deduplicate, and validate Bronze data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from src.config.config_loader import get_config
from src.utils.data_quality import DataQualityRules
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def transform_bronze_to_silver(spark: SparkSession) -> None:
    """
    Transform Bronze to Silver layer.
    
    Steps:
    1. Read Bronze Parquet
    2. Apply data quality rules
    3. Clean strings
    4. Handle nulls
    5. Deduplicate
    6. Validate ranges
    7. Write to Silver
    
    Args:
        spark: SparkSession instance
    """
    config = get_config()
    dq = DataQualityRules()
    
    logger.info(f"Reading Bronze data from: {config.bronze_path}")
    
    # Read Bronze
    df = spark.read.parquet(config.bronze_path)
    initial_count = df.count()
    logger.info(f"Bronze records: {initial_count}")
    
    # Apply transformations
    logger.info("Applying data quality rules...")
    
    # 1. Clean strings
    df = dq.clean_strings(df)
    
    # 2. Handle nulls
    df = dq.handle_nulls(df)
    
    # 3. Deduplicate
    df = dq.deduplicate(df, primary_key="ID")
    
    # 4. Validate ranges
    df = dq.validate_ranges(df)
    
    # 5. Add processing timestamp
    df = df.withColumn("silver_processed_at", current_timestamp())
    
    final_count = df.count()
    logger.info(f"Silver records: {final_count}")
    logger.info(f"Records filtered: {initial_count - final_count}")
    
    # Write to Silver
    logger.info(f"Writing to Silver layer: {config.silver_path}")
    
    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(config.silver_path)
    
    logger.info("Silver transformation complete!")


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session
    
    spark = create_spark_session("Silver-Transformation")
    transform_bronze_to_silver(spark)
    spark.stop()
