"""
Unit tests for Bronze layer ingestion.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from chispa.dataframe_comparer import assert_df_equality
from src.schemas.accidents_schema import get_accidents_schema


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_accidents_schema():
    """Test that schema has expected fields."""
    schema = get_accidents_schema()
    
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 47
    assert "ID" in schema.fieldNames()
    assert "Severity" in schema.fieldNames()
    assert "Start_Time" in schema.fieldNames()


def test_schema_field_types():
    """Test that key fields have correct types."""
    schema = get_accidents_schema()
    
    # Check ID is StringType
    id_field = [f for f in schema.fields if f.name == "ID"][0]
    assert id_field.dataType.typeName() == "string"
    
    # Check Severity is IntegerType
    severity_field = [f for f in schema.fields if f.name == "Severity"][0]
    assert severity_field.dataType.typeName() == "integer"
    
    # Check Start_Time is TimestampType
    time_field = [f for f in schema.fields if f.name == "Start_Time"][0]
    assert time_field.dataType.typeName() == "timestamp"
