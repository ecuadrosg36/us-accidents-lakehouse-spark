"""
Bronze Layer Ingestion.

Reads raw CSV data and writes to Parquet with schema enforcement.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month
from src.config.config_loader import get_config
from src.schemas.accidents_schema import get_accidents_schema
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def ingest_raw_to_bronze(spark: SparkSession) -> None:
    """
    Ingest raw CSV files into Bronze layer.

    Steps:
    1. Read CSV with schema enforcement
    2. Add partition columns (year, month)
    3. Repartition for optimal file sizes
    4. Write to Parquet

    Args:
        spark: SparkSession instance
    """
    config = get_config()
    schema = get_accidents_schema()

    logger.info(f"Reading raw data from: {config.raw_data_path}")

    # Read CSV with schema
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema)
        .load(f"{config.raw_data_path}/*.csv")
    )

    logger.info(f"Loaded {df.count()} records")

    # Add partition columns
    df_partitioned = df.withColumn("year", year("Start_Time")).withColumn(
        "month", month("Start_Time")
    )

    # Repartition for optimal file sizes (targeting 128MB)
    num_partitions = max(1, int(df.count() / 100000))
    logger.info(f"Repartitioning to {num_partitions} partitions")

    df_repartitioned = df_partitioned.repartition(num_partitions, "year", "month")

    # Write to Bronze
    logger.info(f"Writing to Bronze layer: {config.bronze_path}")

    df_repartitioned.write.mode("overwrite").partitionBy("year", "month").parquet(
        config.bronze_path
    )

    logger.info("Bronze ingestion complete!")


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session("Bronze-Ingestion")
    ingest_raw_to_bronze(spark)
    spark.stop()
