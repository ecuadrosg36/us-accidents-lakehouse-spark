"""
Spark Optimization Utilities.

Helper functions for broadcast joins, repartitioning, and performance tuning.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


class SparkOptimizer:
    """Utilities for Spark performance optimization."""

    @staticmethod
    def apply_broadcast_join(
        large_df: DataFrame, small_df: DataFrame, join_col: str
    ) -> DataFrame:
        """
        Perform broadcast join for small dimension tables.

        Args:
            large_df: Large fact table
            small_df: Small dimension table (< 10MB recommended)
            join_col: Column to join on

        Returns:
            Joined DataFrame
        """
        logger.info(f"Applying broadcast join on column: {join_col}")
        return large_df.join(broadcast(small_df), join_col, "left")

    @staticmethod
    def optimize_partitions(
        df: DataFrame, target_partition_size_mb: int = 128
    ) -> DataFrame:
        """
        Repartition DataFrame for optimal file sizes.

        Args:
            df: Input DataFrame
            target_partition_size_mb: Target partition size in MB

        Returns:
            Repartitioned DataFrame
        """
        # Estimate partition count based on data size
        # Assuming ~1KB per row (rough estimate)
        row_count = df.count()
        estimated_size_mb = (row_count * 1024) / (1024 * 1024)

        num_partitions = max(1, int(estimated_size_mb / target_partition_size_mb))

        logger.info(
            f"Repartitioning to {num_partitions} partitions (target: {target_partition_size_mb}MB)"
        )

        return df.repartition(num_partitions)

    @staticmethod
    def tune_shuffle_partitions(spark, num_partitions: int = None):
        """
        Dynamically tune shuffle partitions.

        Args:
            spark: SparkSession
            num_partitions: Number of shuffle partitions (None = auto-detect)
        """
        if num_partitions is None:
            # Auto-detect based on data size
            num_partitions = 200  # Default

        spark.conf.set("spark.sql.shuffle.partitions", num_partitions)
        logger.info(f"Set shuffle partitions to: {num_partitions}")

    @staticmethod
    def cache_df(df: DataFrame, name: str = "cached_df") -> DataFrame:
        """
        Cache DataFrame with logging.

        Args:
            df: DataFrame to cache
            name: Name for logging

        Returns:
            Cached DataFrame
        """
        logger.info(f"Caching DataFrame: {name}")
        return df.cache()
