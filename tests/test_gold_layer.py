"""
Unit tests for Gold layer features.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    TimestampType,
    DoubleType,
)
from datetime import datetime
from src.utils.feature_engineering import FeatureEngineer


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder.appName("test-gold").master("local[1]").getOrCreate()
    yield spark
    spark.stop()


def test_temporal_features(spark):
    """Test temporal feature creation."""
    fe = FeatureEngineer()

    # Create test data with known timestamp
    data = [(datetime(2023, 1, 15, 8, 30),)]  # Sunday, 8:30 AM
    schema = StructType([StructField("Start_Time", TimestampType(), True)])

    df = spark.createDataFrame(data, schema)
    df_features = fe.create_temporal_features(df)

    row = df_features.collect()[0]

    # Check hour
    assert row["hour_of_day"] == 8

    # Check weekend (Sunday = 1)
    assert row["is_weekend"] == 1

    # Check rush hour (8 AM is rush hour)
    assert row["is_rush_hour"] == 1


def test_weather_features(spark):
    """Test weather feature creation."""
    fe = FeatureEngineer()

    data = [
        (25.0, 0.5, 35.0),  # Extreme temp, poor visibility, heavy wind
        (70.0, 10.0, 15.0),  # Normal conditions
    ]
    schema = StructType(
        [
            StructField("Temperature(F)", DoubleType(), True),
            StructField("Visibility(mi)", DoubleType(), True),
            StructField("Wind_Speed(mph)", DoubleType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    df_features = fe.create_weather_features(df)

    rows = df_features.collect()

    # First row should flag extreme conditions
    assert rows[0]["extreme_temp"] == 1
    assert rows[0]["poor_visibility"] == 1
    assert rows[0]["heavy_wind"] == 1

    # Second row should be normal
    assert rows[1]["extreme_temp"] == 0
    assert rows[1]["poor_visibility"] == 0
    assert rows[1]["heavy_wind"] == 0
