"""
Unit tests for Silver layer transformation.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from src.utils.data_quality import DataQualityRules


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder.appName("test-silver").master("local[1]").getOrCreate()
    yield spark
    spark.stop()


def test_deduplicate(spark):
    """Test deduplication logic."""
    dq = DataQualityRules()

    # Create test data with duplicates
    data = [
        ("1", "A"),
        ("1", "A"),  # Duplicate
        ("2", "B"),
    ]
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    df_dedup = dq.deduplicate(df, primary_key="ID")

    assert df_dedup.count() == 2


def test_validate_ranges(spark):
    """Test range validation."""
    dq = DataQualityRules()

    data = [
        ("1", 1, 40.0, -80.0),  # Valid
        ("2", 5, 40.0, -80.0),  # Invalid severity
        ("3", 2, 95.0, -80.0),  # Invalid latitude
    ]
    schema = StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Severity", IntegerType(), True),
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    df_valid = dq.validate_ranges(df)

    # Should filter out invalid records
    assert df_valid.count() == 1


def test_clean_strings(spark):
    """Test string cleaning."""
    dq = DataQualityRules()

    data = [("  New York  ", "ny")]
    schema = StructType(
        [
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)
    df_clean = dq.clean_strings(df)

    # Check trimming
    assert df_clean.collect()[0]["City"] == "New York"
    # Check uppercasing
    assert df_clean.collect()[0]["State"] == "NY"
