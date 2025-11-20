"""
Data Quality Rules for Silver Layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


class DataQualityRules:
    """Data quality validation and cleaning rules."""
    
    @staticmethod
    def clean_strings(df: DataFrame) -> DataFrame:
        """
        Clean string columns.
        
        - Trim whitespace
        - Standardize to uppercase for codes
        """
        string_cols = ["City", "State", "County", "Street", "Country", "Timezone"]
        
        for col_name in string_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, trim(col(col_name)))
        
        # Uppercase state/country codes
        if "State" in df.columns:
            df = df.withColumn("State", upper(col("State")))
        if "Country" in df.columns:
            df = df.withColumn("Country", upper(col("Country")))
            
        return df
    
    @staticmethod
    def handle_nulls(df: DataFrame) -> DataFrame:
        """
        Handle null values based on business logic.
        
        - Drop rows with null Severity, Start_Time, or Location
        - Fill numeric nulls with 0 or median
        """
        # Critical fields - drop if null
        critical_fields = ["ID", "Severity", "Start_Time", "Start_Lat", "Start_Lng"]
        df = df.dropna(subset=critical_fields)
        
        # Fill numeric weather fields with 0
        weather_fields = [
            "Temperature(F)", "Humidity(%)", "Pressure(in)", 
            "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)"
        ]
        
        for field in weather_fields:
            if field in df.columns:
                df = df.fillna({field: 0.0})
        
        return df
    
    @staticmethod
    def validate_ranges(df: DataFrame) -> DataFrame:
        """
        Validate and filter outliers.
        
        - Severity: 1-4
        - Latitude: -90 to 90
        - Longitude: -180 to 180
        - Temperature: -50 to 150 F
        """
        df = df.filter(
            (col("Severity").between(1, 4)) &
            (col("Start_Lat").between(-90, 90)) &
            (col("Start_Lng").between(-180, 180))
        )
        
        # Flag extreme temperatures but don't drop
        if "Temperature(F)" in df.columns:
            df = df.withColumn(
                "temp_valid",
                when(col("Temperature(F)").between(-50, 150), True).otherwise(False)
            )
        
        return df
    
    @staticmethod
    def deduplicate(df: DataFrame, primary_key: str = "ID") -> DataFrame:
        """
        Remove duplicate records.
        
        Args:
            df: Input DataFrame
            primary_key: Column to use for deduplication
            
        Returns:
            Deduplicated DataFrame
        """
        initial_count = df.count()
        df_dedup = df.dropDuplicates([primary_key])
        final_count = df_dedup.count()
        
        duplicates_removed = initial_count - final_count
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate records")
        
        return df_dedup
