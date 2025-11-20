"""
Feature Engineering for Gold Layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    when,
    sqrt,
    abs,
    concat_ws,
    length,
    datediff,
    unix_timestamp,
)
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


class FeatureEngineer:
    """Create ML-ready features from Silver data."""

    @staticmethod
    def create_temporal_features(df: DataFrame) -> DataFrame:
        """
        Extract temporal features from Start_Time.

        Features:
        - hour_of_day
        - day_of_week (1=Monday, 7=Sunday)
        - is_weekend
        - is_rush_hour (7-9 AM, 4-7 PM)
        """
        df = df.withColumn("hour_of_day", hour("Start_Time"))
        df = df.withColumn("day_of_week", dayofweek("Start_Time"))

        # Weekend flag
        df = df.withColumn(
            "is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)
        )

        # Rush hour flag
        df = df.withColumn(
            "is_rush_hour",
            when(
                (col("hour_of_day").between(7, 9))
                | (col("hour_of_day").between(16, 19)),
                1,
            ).otherwise(0),
        )

        return df

    @staticmethod
    def create_location_features(df: DataFrame) -> DataFrame:
        """
        Create location-based features.

        Features:
        - lat_lng (combined coordinate)
        - distance_moved (difference between start/end)
        """
        # Combined lat/lng as string (for clustering/grouping)
        df = df.withColumn(
            "lat_lng", concat_ws(",", col("Start_Lat"), col("Start_Lng"))
        )

        # Distance moved (Euclidean approximation)
        df = df.withColumn(
            "distance_moved",
            when(
                col("End_Lat").isNotNull() & col("End_Lng").isNotNull(),
                sqrt(
                    (col("End_Lat") - col("Start_Lat")) ** 2
                    + (col("End_Lng") - col("Start_Lng")) ** 2
                ),
            ).otherwise(0.0),
        )

        return df

    @staticmethod
    def create_weather_features(df: DataFrame) -> DataFrame:
        """
        Create weather-based features.

        Features:
        - extreme_temp (outside 32-100F)
        - poor_visibility (< 2 miles)
        - heavy_wind (> 30 mph)
        """
        df = df.withColumn(
            "extreme_temp",
            when(
                (col("Temperature(F)") < 32) | (col("Temperature(F)") > 100), 1
            ).otherwise(0),
        )

        df = df.withColumn(
            "poor_visibility", when(col("Visibility(mi)") < 2, 1).otherwise(0)
        )

        df = df.withColumn(
            "heavy_wind", when(col("Wind_Speed(mph)") > 30, 1).otherwise(0)
        )

        return df

    @staticmethod
    def create_infrastructure_features(df: DataFrame) -> DataFrame:
        """
        Create infrastructure features.

        Features:
        - infrastructure_count (sum of boolean flags)
        """
        infrastructure_cols = [
            "Amenity",
            "Bump",
            "Crossing",
            "Give_Way",
            "Junction",
            "Railway",
            "Roundabout",
            "Station",
            "Stop",
            "Traffic_Signal",
        ]

        # Count total infrastructure elements
        from functools import reduce
        from pyspark.sql.functions import coalesce, lit

        df = df.withColumn(
            "infrastructure_count",
            reduce(
                lambda a, b: a + b,
                [
                    coalesce(col(c).cast("int"), lit(0))
                    for c in infrastructure_cols
                    if c in df.columns
                ],
            ),
        )

        return df

    @staticmethod
    def select_ml_features(df: DataFrame) -> DataFrame:
        """
        Select final ML-ready columns.
        """
        feature_cols = [
            # Target
            "Severity",
            # Temporal
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "is_rush_hour",
            # Location
            "Start_Lat",
            "Start_Lng",
            "State",
            # Weather
            "Temperature(F)",
            "Humidity(%)",
            "Visibility(mi)",
            "Wind_Speed(mph)",
            "extreme_temp",
            "poor_visibility",
            "heavy_wind",
            # Infrastructure
            "infrastructure_count",
            # Original
            "Distance(mi)",
            # Identifiers (for joins)
            "ID",
            "year",
            "month",
        ]

        # Only select columns that exist
        existing_cols = [c for c in feature_cols if c in df.columns]

        return df.select(existing_cols)
