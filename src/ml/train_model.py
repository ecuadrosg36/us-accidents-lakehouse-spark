"""
ML Model Training Pipeline.

Train RandomForest model to predict accident severity.
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from src.config.config_loader import get_config
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def train_severity_model(spark: SparkSession) -> None:
    """
    Train model to predict accident severity.
    
    Features used:
    - hour_of_day, day_of_week
    - is_weekend, is_rush_hour
    - Temperature, Humidity, Visibility, Wind_Speed
    - infrastructure_count
    - Distance
    
    Target: Severity (1-4)
    
    Args:
        spark: SparkSession instance
    """
    config = get_config()
    
    logger.info(f"Loading Gold data from: {config.gold_path}")
    
    # Load Gold data
    df = spark.read.parquet(config.gold_path)
    
    # Select features for training
    feature_cols = [
        "hour_of_day", "day_of_week", "is_weekend", "is_rush_hour",
        "Temperature(F)", "Humidity(%)", "Visibility(mi)", "Wind_Speed(mph)",
        "infrastructure_count", "Distance(mi)",
        "extreme_temp", "poor_visibility", "heavy_wind"
    ]
    
    # Filter to ensure we have all features
    df = df.select(["Severity"] + feature_cols).na.drop()
    
    logger.info(f"Training samples: {df.count()}")
    
    # Split data
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    
    logger.info(f"Train size: {train_df.count()}, Test size: {test_df.count()}")
    
    # Build pipeline
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    
    rf = RandomForestClassifier(
        labelCol="Severity",
        featuresCol="features",
        numTrees=50,
        maxDepth=10,
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, rf])
    
    # Train model
    logger.info("Training RandomForest model...")
    model = pipeline.fit(train_df)
    
    # Evaluate
    predictions = model.transform(test_df)
    
    evaluator = MulticlassClassificationEvaluator(
        labelCol="Severity",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    accuracy = evaluator.evaluate(predictions)
    logger.info(f"Model Accuracy: {accuracy:.4f}")
    
    # Save model
    model_path = f"{config.model_path}/severity_model"
    logger.info(f"Saving model to: {model_path}")
    model.write().overwrite().save(model_path)
    
    logger.info("Model training complete!")


if __name__ == "__main__":
    from src.utils.spark_session import create_spark_session
    
    spark = create_spark_session("ML-Training")
    train_severity_model(spark)
    spark.stop()
