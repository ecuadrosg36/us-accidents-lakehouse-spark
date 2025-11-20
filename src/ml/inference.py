"""
ML Inference Pipeline.

Load trained model and make predictions.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.ml import PipelineModel
from src.config.config_loader import get_config
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def predict_severity(spark: SparkSession, input_df: DataFrame = None) -> DataFrame:
    """
    Make severity predictions on new data.

    Args:
        spark: SparkSession instance
        input_df: Optional input DataFrame (defaults to Gold data)

    Returns:
        DataFrame with predictions
    """
    config = get_config()

    # Load model
    model_path = f"{config.model_path}/severity_model"
    logger.info(f"Loading model from: {model_path}")

    model = PipelineModel.load(model_path)

    # Load data if not provided
    if input_df is None:
        logger.info(f"Loading Gold data from: {config.gold_path}")
        input_df = spark.read.parquet(config.gold_path)

    # Make predictions
    logger.info("Generating predictions...")
    predictions = model.transform(input_df)

    # Select relevant columns
    result = predictions.select(
        "ID", "Severity", "prediction", "probability", "Start_Time", "State"
    )

    logger.info(f"Predictions generated: {result.count()}")

    return result


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session

    spark = create_spark_session("ML-Inference")

    # Run inference
    predictions = predict_severity(spark)

    # Show sample predictions
    predictions.show(10, truncate=False)

    spark.stop()
