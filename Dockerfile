# Start with the official Airflow image
FROM apache/airflow:2.8.1

# Switch to root to install system dependencies (if needed)
USER root
RUN apt-get update && \
    apt-get install -y libaio1 && \
    apt-get clean

# Switch back to airflow user to install Python packages
USER airflow

# Install the required libraries for our project
RUN pip install --no-cache-dir \
    oracledb \
    faker \
    pandas