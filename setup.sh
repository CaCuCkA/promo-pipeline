#!/bin/bash

# Create directories
mkdir -p ./logs ./dags ./plugins

# Set environment variables in .env file
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env

# Move Python file to dags directory
mv image_domain_scraper.py ./dags/
