# US Accidents Spark Lakehouse 🚀

An **Apache Spark** lakehouse for processing and analyzing US traffic accident data using the **Medallion Architecture** (Bronze → Silver → Gold).

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Spark](https://img.shields.io/badge/Spark-3.5%2B-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.7%2B-green)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)

## 🌟 Features

### Working today

- **Medallion layers**: Bronze (raw) → Silver (cleaned) → Gold (ML-ready), stored as Parquet and partitioned by year and month
- **Spark settings**: adaptive query execution (AQE) with partition coalescing
- **ML**: Spark ML RandomForest that predicts severity and saves the model
- **Orchestration**: a daily Airflow DAG (TaskFlow API): Bronze → Silver → Gold → training, which only runs on Mondays
- **CI**: GitHub Actions runs Black, Flake8, mypy and pytest, then builds the Docker image

### Written, not wired in yet

- **Streaming**: `src/jobs/stream_ingestion.py` reads CSV files with Structured Streaming (watermark and checkpoint), but the DAG doesn't run it
- **Inference**: `src/ml/inference.py` prints 10 predictions and doesn't save them
- **Broadcast joins**: `SparkOptimizer.apply_broadcast_join` exists but no job calls it
- **Terraform**: `infra/terraform/` only declares two variables; `main.tf` has no resources yet
- **Placeholders**: `src/main_pipeline.py`, `orchestration/airflow/dags/lakeshield_dag.py` and `scripts/download_data.py` only print a line

## 🏗️ Architecture

```
data/raw (CSV)
    ↓
Bronze Layer (Parquet, schema-enforced, partitioned)
    ↓
Silver Layer (cleaned, deduplicated, validated)
    ↓
Gold Layer (ML features, optimized)
    ↓
ML Model (RandomForest classifier)
```

All layers are Parquet files on disk (not Delta tables).

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Apache Spark 3.5+
- Docker (optional)

### Installation

```bash
# Clone repository
git clone https://github.com/ecuadrosg36/us-accidents-lakehouse-spark.git
cd us-accidents-lakehouse-spark

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
```

### Run the Pipeline

**1. Bronze Layer (CSV → Parquet)**
```bash
python -m src.jobs.bronze_ingestion
```

**2. Silver Layer (Data Quality)**
```bash
python -m src.jobs.silver_transformation
```

**3. Gold Layer (Features)**
```bash
python -m src.jobs.gold_features
```

**4. ML Training**
```bash
python -m src.ml.train_model
```

**5. ML Inference** (prints sample predictions)
```bash
python -m src.ml.inference
```

## 📁 Project Structure

```
.
├── src/
│   ├── config/           # Configuration loader
│   ├── schemas/          # Data schemas
│   ├── jobs/             # ETL jobs (Bronze/Silver/Gold)
│   ├── ml/               # ML training & inference
│   └── utils/            # Spark session, logging, optimization
├── orchestration/
│   └── airflow/dags/     # Airflow DAG (lakehouse_pipeline.py)
├── infra/
│   └── terraform/        # Variables only, no resources yet
├── tests/                # Unit tests
├── config/               # YAML configs
└── Dockerfile            # Docker image
```

## 🐳 Docker

```bash
# Build image
docker build -t us-accidents-spark:latest .

# Run Bronze job
docker run --rm -v $(pwd)/data:/app/data us-accidents-spark:latest
```

The image is `python:3.10-slim` without Java, so Spark jobs need a Java runtime added before they run in it. CI only checks that the image builds and starts Python.

## ☁️ Cloud Deployment (GCP)

Not built yet. `infra/terraform/` has the `project_id` and `region` variables but no Dataproc or storage resources.

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

## 📊 Data Quality

- **Silver Layer DQ Rules**:
  - Null handling (drop critical, fill weather)
  - Deduplication (by ID)
  - Range validation (Severity 1-4, Lat/Lng bounds)
  - String cleaning (trim, uppercase codes)

## 🤖 ML Model

- **Target**: Accident Severity (1-4)
- **Algorithm**: RandomForest (50 trees, depth 10)
- **Features**: 
  - Temporal (hour, day_of_week, is_weekend, is_rush_hour)
  - Weather (temperature, humidity, visibility, wind)
  - Infrastructure (count of traffic elements)
  - Location (lat/lng)

## 🔗 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open a Pull Request

## 📝 License

MIT License
