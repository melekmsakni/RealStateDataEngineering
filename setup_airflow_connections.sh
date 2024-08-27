#!/bin/bash

# Ensure Airflow is installed
if ! command -v airflow &> /dev/null
then
    echo "Airflow could not be found. Please install Airflow before running this script."
    exit
fi



# Set up Spark connection
airflow connections
 add \
    --conn_id spark_container \
    --conn_type spark \
    --conn_host spark://spark-master \
    --conn_port 7077

echo "Connections have been set up successfully."
