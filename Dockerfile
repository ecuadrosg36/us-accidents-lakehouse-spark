# Use official Spark base image
FROM apache/spark-py:3.5.0

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Set environment variables
ENV PYTHONPATH=/app
ENV SPARK_HOME=/opt/spark

# Default command (can be overridden)
CMD ["python", "-m", "src.jobs.bronze_ingestion"]
