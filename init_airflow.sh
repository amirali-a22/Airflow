#!/bin/bash

# Create essential Airflow folders if they don't exist
mkdir -p ./dags ./logs ./plugins

# Set AIRFLOW_UID for Docker (if not already set)
[ ! -f .env ] && echo -e "AIRFLOW_UID=$(id -u)" > .env

# Download Airflow's Docker Compose file (if not present)
[ ! -f docker-compose.yaml ] && curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Start Airflow services
docker compose up -d

# Verify running containers
docker compose ps