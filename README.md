# US Accidents Spark Lakehouse 🚀

A production-ready **Apache Spark Lakehouse** platform for processing and analyzing US traffic accident data using the **Medallion Architecture** (Bronze → Silver → Gold).

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Spark](https://img.shields.io/badge/Spark-3.5%2B-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.7%2B-green)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)

## 🌟 Features

- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (ML-ready)
- **Spark Optimization**: AQE, broadcast joins, dynamic partitioning
- **ML Pipeline**: RandomForest model for severity prediction
- **Streaming**: Structured Streaming with checkpointing & watermarking
- **Orchestration**: Airflow DAG with TaskFlow API
- **Infrastructure as Code**: Terraform for GCP/Dataproc
- **CI/CD**: GitHub Actions with testing & Docker builds

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

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Apache Spark 3.5+
- Docker (optional)

### Installation

```bash
# Clone repository
git clone <repo-url>
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

**5. ML Inference**
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
│   └── airflow/dags/     # Airflow DAGs
├── infra/
│   └── terraform/        # Infrastructure as Code
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

## ☁️ Cloud Deployment (GCP)

```bash
cd infra/terraform

# Initialize
terraform init

# Plan
terraform plan -var="project_id=YOUR_PROJECT_ID"

# Apply
terraform apply -var="project_id=YOUR_PROJECT_ID"
```

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
