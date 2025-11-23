# Start from the official Airflow image
FROM apache/airflow:3.0.3

# Switch to root user to install system-level packages
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       postgresql-client \
       libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Back to non-root airflow user
USER airflow

# Install Python dependencies
COPY --chown=airflow:root requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

