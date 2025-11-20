"""
Airflow DAG for Lakehouse Pipeline.

Orchestrates Bronze → Silver → Gold → ML workflow.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators import python


# Default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="us_accidents_lakehouse_pipeline",
    default_args=default_args,
    description="US Accidents Lakehouse ETL + ML Pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "spark", "ml"],
)
def accidents_pipeline():
    """
    Daily pipeline for processing US Accidents data.

    Steps:
    1. Bronze ingestion (CSV → Parquet)
    2. Silver transformation (cleaning, DQ)
    3. Gold features (ML-ready)
    4. ML training (optional, weekly)
    """

    @task()
    def bronze_layer():
        """Ingest raw data to Bronze."""
        from src.utils.spark_session import create_spark_session
        from src.jobs.bronze_ingestion import ingest_raw_to_bronze

        spark = create_spark_session("Airflow-Bronze")
        ingest_raw_to_bronze(spark)
        spark.stop()

        return "Bronze ingestion complete"

    @task()
    def silver_layer():
        """Transform Bronze to Silver."""
        from src.utils.spark_session import create_spark_session
        from src.jobs.silver_transformation import transform_bronze_to_silver

        spark = create_spark_session("Airflow-Silver")
        transform_bronze_to_silver(spark)
        spark.stop()

        return "Silver transformation complete"

    @task()
    def gold_layer():
        """Create Gold features."""
        from src.utils.spark_session import create_spark_session
        from src.jobs.gold_features import transform_silver_to_gold

        spark = create_spark_session("Airflow-Gold")
        transform_silver_to_gold(spark)
        spark.stop()

        return "Gold features complete"

    @task()
    def ml_training():
        """Train ML model (runs weekly)."""
        from src.utils.spark_session import create_spark_session
        from src.ml.train_model import train_severity_model

        # Check if today is Monday (weekly training)
        if datetime.today().weekday() == 0:
            spark = create_spark_session("Airflow-ML")
            train_severity_model(spark)
            spark.stop()
            return "ML training complete"
        else:
            return "ML training skipped (not Monday)"

    # Define task dependencies
    bronze = bronze_layer()
    silver = silver_layer()
    gold = gold_layer()
    ml = ml_training()

    bronze >> silver >> gold >> ml


# Instantiate DAG
dag_instance = accidents_pipeline()
