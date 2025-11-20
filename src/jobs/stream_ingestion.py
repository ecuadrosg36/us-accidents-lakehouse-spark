"""
Streaming Ingestion Pipeline.

Auto-ingest new CSV files to Bronze layer using Structured Streaming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, year, month
from src.config.config_loader import get_config
from src.schemas.accidents_schema import get_accidents_schema
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def streaming_bronze_ingestion(spark: SparkSession) -> None:
    """
    Structured Streaming job for Bronze ingestion.

    Features:
    - Auto-detect new CSV files
    - Checkpointing for fault tolerance
    - Watermarking for late data
    - Write to Bronze in append mode

    Args:
        spark: SparkSession instance
    """
    config = get_config()
    schema = get_accidents_schema()

    logger.info(f"Starting streaming ingestion from: {config.raw_data_path}")

    # Read stream with schema
    stream_df = (
        spark.readStream.format("csv")
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("maxFilesPerTrigger", 1)
        .schema(schema)
        .load(f"{config.raw_data_path}")
    )

    # Add metadata
    stream_df = (
        stream_df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("year", year("Start_Time"))
        .withColumn("month", month("Start_Time"))
    )

    # Apply watermarking (allow 1 hour late data)
    stream_df = stream_df.withWatermark("Start_Time", "1 hour")

    # Write stream to Bronze
    logger.info(f"Writing stream to Bronze: {config.bronze_path}")

    query = (
        stream_df.writeStream.format("parquet")
        .option("checkpointLocation", f"{config.checkpoint_path}/bronze")
        .option("path", config.bronze_path)
        .partitionBy("year", "month")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info(
        "Streaming job started. Checkpoint: {}/bronze".format(config.checkpoint_path)
    )

    # Wait for termination (for demo purposes)
    # In production, this would run indefinitely
    query.awaitTermination(timeout=60)  # Run for 1 minute

    logger.info("Streaming job completed")


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session("Streaming-Bronze")
    streaming_bronze_ingestion(spark)
    spark.stop()
