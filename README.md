


# 🏠🇹🇳 Tunisia Real Estate Data Pipeline



Welcome to our cutting-edge Tunisian Real Estate Data Pipeline! This project seamlessly integrates the power of Apache Kafka, Spark, Airflow, and Superset to process and visualize real estate data in real-time. By combining data from diverse sources, we offer a holistic view of the Tunisian property market, enabling data-driven decision-making for investors, agents, and policymakers alike. 🚀
Our pipeline is designed to:

- Aggregate data from multiple Tunisian real estate sources
- Process and analyze information in near real-time
- Generate comprehensive insights into market trends
- Provide interactive visualizations for intuitive data exploration

Whether you're tracking property prices, monitoring market fluctuations, or identifying emerging hotspots, our Tunisia Real Estate Data Pipeline is your go-to solution for staying ahead in the dynamic world of Tunisian real estate.

[![Made with Apache Kafka](https://img.shields.io/badge/Made%20with-Apache%20Kafka-black?style=flat-square&logo=apache-kafka)](https://kafka.apache.org/)
[![Powered by Apache Spark](https://img.shields.io/badge/Powered%20by-Apache%20Spark-orange?style=flat-square&logo=apache-spark)](https://spark.apache.org/)
[![Orchestrated with Apache Airflow](https://img.shields.io/badge/Orchestrated%20with-Apache%20Airflow-blue?style=flat-square&logo=apache-airflow)](https://airflow.apache.org/)
[![Visualized with Apache Superset](https://img.shields.io/badge/Visualized%20with-Apache%20Superset-green?style=flat-square&logo=apache-superset)](https://superset.apache.org/)
[![Azure Cloud Services](https://img.shields.io/badge/Cloud-Azure-blue?style=flat-square&logo=microsoft-azure)](https://azure.microsoft.com/)
[![Stored in Apache Cassandra](https://img.shields.io/badge/Stored%20in-Apache%20Cassandra-1287B1?style=flat-square&logo=apache-cassandra)](https://cassandra.apache.org/)


## 📚 Table of Contents

- [🌟 Introduction](#-introduction)
- [🏗️ Architecture](#️-architecture)
- [📊 Visualization Dashboard](#-visualization-dashboard)
- [🛠️ Prerequisites](#️-prerequisites)
- [🚀 Installation](#-installation)
- [🔧 Project Setup](#-project-setup)
- [🌈 Environment Variables](#-environment-variables)
- [💾 Data Persistence](#-data-persistence)
- [🐛 Troubleshooting](#-troubleshooting)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

## 🌟 Introduction

Dive into the dynamic world of Tunisian real estate analytics with our comprehensive data pipeline! This project harnesses data from multiple prominent sources including Tecnocasa, Remxx, and other key players in the Tunisian real estate market. Our robust pipeline ensures real-time analytics and delivers actionable insights into the ever-evolving property landscape. From data ingestion to insightful visualizations, we've got you covered! 📊🏘️

## 🏗️ Architecture

Here's a high-level overview of our pipeline architecture:





![data_archi](https://github.com/user-attachments/assets/e53f4967-0ddc-4b4c-8913-c842d2dbdfa7)

## 📊 Visualization Dashboard

Our interactive visualization dashboard, powered by Apache Superset, provides real-time insights into the Tunisian real estate market.

![dashborad](https://github.com/user-attachments/assets/a8591ad4-f24a-491e-93b0-4c26e7c00ba6)

## 🛠️ Prerequisites

Before embarking on this data journey, make sure you have:

- **Git** 🐙
- **Docker** 🐳 ([Install Docker](https://docs.docker.com/get-docker/))
- **Docker Compose** 🐋 (For Linux users):

  ```bash
  sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
  sudo chmod +x /usr/local/bin/docker-compose
  docker-compose --version
  ```

## 🚀 Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/real-estate-pipeline.git
   cd real-estate-pipeline
   ```

2. Install Python requirements (for local development):
   ```bash
   pip install -r requirements.txt
   ```

## 🔧 Project Setup

1. Initialize Airflow:
   ```bash
   docker-compose up airflow-init
   ```

2. Launch the pipeline:
   ```bash
   docker-compose up --build -d
   ```

3. Access the services:
   - 🎨 Apache Superset: http://localhost:8088
   - 🌬️ Airflow Web UI: http://localhost:8080
   - 🎛️ Kafka Control Center: http://localhost:9021

4. Stop the pipeline:
   ```bash
   docker-compose down
   ```

## 🌈 Environment Variables

Customize your pipeline by tweaking the `.env` file. It's like choosing the perfect paint color for your house! 🎨

## 💾 Data Persistence

We use Docker volumes to keep your data safe and sound, even when containers take a nap. 😴

## 🐛 Troubleshooting

If things go sideways, check the logs:
```bash
docker-compose logs -f <service_name>
```

For Airflow logs:
```bash
docker-compose logs -f airflow
```

## 🤝 Contributing

Got ideas? We love them! Fork the repo, make your changes, and send us a pull request. Let's build something amazing together! 🤜🤛

## 📄 License

This project is licensed under the MIT License. Check out the LICENSE file for the fine print.

---

Built with ❤️ by Melek Msakni
