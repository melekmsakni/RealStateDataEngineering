# Real Estate Data Pipeline

Welcome to the Real Estate Data Pipeline project! This pipeline is designed to aggregate, process, and visualize real estate data in real-time, utilizing cutting-edge technologies like Apache Kafka, Apache Spark, Apache Airflow, and Apache Superset. The entire pipeline is containerized using Docker and orchestrated with Docker Compose.

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Setup](#project-setup)
  - [Clone the Repository](#clone-the-repository)
  - [Install Python Requirements](#install-python-requirements)
  - [Initialize and Run the Pipeline](#initialize-and-run-the-pipeline)
  - [Accessing the Services](#accessing-the-services)
  - [Stopping the Pipeline](#stopping-the-pipeline)
- [Environment Variables](#environment-variables)
- [Data Persistence](#data-persistence)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project is a comprehensive real estate data pipeline, capable of processing and visualizing real-time data. The goal is to provide insights into real estate trends by collecting data from various sources, processing it with Apache Spark, storing it in Cassandra, and visualizing the results with Apache Superset.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Git**: To clone the repository.
- **Docker**: To run containers. Installation instructions can be found [here](https://docs.docker.com/get-docker/).
- **Docker Compose**: For orchestrating multiple containers. If you're using Linux or need to install it separately, follow these steps:

  ```bash
  sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
  sudo chmod +x /usr/local/bin/docker-compose
  docker-compose --version
