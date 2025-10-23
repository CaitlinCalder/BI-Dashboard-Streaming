# BI-Dashboard-Streaming

<div align="center">

<img src="BI-Dashboard-Streaming.png" width="30%" alt="Project Logo"/>

# BI-Dashboard-Streaming

### Real-Time Business Intelligence Platform

<div style="margin: 20px 0;">

![License](https://img.shields.io/badge/License-MIT-0078D4?style=flat&logo=opensourceinitiative&logoColor=white)
![Last Commit](https://img.shields.io/github/last-commit/CaitlinCalder/BI-Dashboard-Streaming?style=flat&logo=git&logoColor=white&color=0078D4)
![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=flat&logo=python&logoColor=white)
![Version](https://img.shields.io/badge/Version-1.0.0-0052CC?style=flat)

</div>

### Built with the tools and technologies:

![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=FastAPI&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=Docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apache-kafka&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=flat&logo=mongodb&logoColor=white)
![PowerBI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Pydantic](https://img.shields.io/badge/Pydantic-E92063?style=flat&logo=Pydantic&logoColor=white)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Testing](#testing)
- [Features](#features)
- [Project Structure](#project-structure)
  - [Project Index](#project-index)
- [License](#license)

---

## Overview

BI-Dashboard-Streaming is an enterprise-grade real-time data pipeline solution that enables seamless synchronization from MongoDB to Power BI dashboards via Apache Kafka. This platform transforms raw database changes into actionable business intelligence with millisecond latency, providing organizations with live analytics capabilities.

**Key Value Propositions:**

- **Real-time Data Streaming**: Continuously captures MongoDB changes and streams updates to Kafka and Power BI
- **Kafka Ecosystem Integration**: Orchestrates Kafka components for scalable data pipelines
- **Comprehensive Diagnostics**: Provides tools to validate configurations, schemas, and data flow health
- **Centralized Configuration**: Simplifies environment setup for MongoDB and Kafka connections
- **Automated Power BI Refresh**: Facilitates seamless dataset updates for current visualizations
- **System Monitoring**: Offers comprehensive health checks to ensure data pipeline integrity

---

## Features

| Component | Details |
| :--- | :--- |
| **Architecture** | Streaming data pipeline integrating Kafka, FastAPI, and MongoDB. Microservices-based with decoupled components. Containerized via Docker Compose for orchestration. |
| **Code Quality** | Consistent Python code adhering to PEP 8 standards. Use of Pydantic models for data validation. Clear modular separation of API, Kafka consumers, and database interactions. |
| **Documentation** | Comprehensive `docker-compose.yml` for environment setup. README includes architecture overview and setup instructions. Schema documentation via `kafka_message_schema.json`. |
| **Integrations** | Kafka for streaming data ingestion and message passing. MongoDB for persistent storage. Power BI for visualization. FastAPI and Uvicorn for REST API endpoints. Websockets for real-time dashboard updates. |
| **Modularity** | Separate modules for Kafka consumers, API endpoints, and database access. Reusable Pydantic models for message schemas. Docker Compose manages multi-container setup. |
| **Testing** | Implied unit testing via Python scripts. Potential for integration tests with Docker environment. Schema validation with `kafka_message_schema.json`. |
| **Performance** | Asynchronous WebSocket handling with `websockets` library. Kafka's high-throughput messaging for streaming. Dockerized environment supports scalable deployment. |
| **Security** | Environment variables via `python-dotenv` for sensitive configs. Potential for secure Kafka and MongoDB connections. |
| **Dependencies** | Python dependencies managed via `requirements.txt`. Key packages include `fastapi`, `kafka-python`, `pydantic`, `pymongo`, `websockets`, `requests`. |

---

## Project Structure

```
└── BI-Dashboard-Streaming/
    ├── CMPG321_PowerBI.pbix
    ├── ClearVueConfig.py
    ├── README.md
    ├── bi_dashboard.py
    ├── clearvue_streaming.log
    ├── diagnostic_results.json
    ├── docker-compose.yml
    ├── kafka-plugins
    │   └── mongo-kafka-connect-1.10.1-all.jar.jar
    ├── kafka_message_schema.json
    ├── mongodb_kafka_streaming.py
    ├── requirements.txt
    ├── server.py
    ├── verify_bi.py
    └── verify_data.py
```

---

### Project Index

<details open>
<summary><b>BI-DASHBOARD-STREAMING/</b></summary>

<div style="padding: 8px 0; color: #666;">
<table style="width: 100%; border-collapse: collapse;">
<thead>
    <tr style="background-color: #f8f9fa;">
        <th style="width: 30%; text-align: left; padding: 8px;">File Name</th>
        <th style="text-align: left; padding: 8px;">Summary</th>
    </tr>
</thead>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/CMPG321_PowerBI.pbix'>CMPG321_PowerBI.pbix</a></b></td>
        <td style="padding: 8px;">Power BI dashboard file for data visualization and analytics.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/kafka_message_schema.json'>kafka_message_schema.json</a></b></td>
        <td style="padding: 8px;">Defines structured schema for Kafka messages used in real-time Power BI dashboards.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/server.py'>server.py</a></b></td>
        <td style="padding: 8px;">Central API server for the platform, integrating dashboard interface with transaction generator.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/docker-compose.yml'>docker-compose.yml</a></b></td>
        <td style="padding: 8px;">Defines and orchestrates Kafka-based streaming architecture with core components.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/ClearVueConfig.py'>ClearVueConfig.py</a></b></td>
        <td style="padding: 8px;">Centralized configuration management for MongoDB Atlas and Kafka integrations.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/verify_bi.py'>verify_bi.py</a></b></td>
        <td style="padding: 8px;">Comprehensive diagnostics for Power BI streaming data integration and validation.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/bi_dashboard.py'>bi_dashboard.py</a></b></td>
        <td style="padding: 8px;">Facilitates Power BI data refresh setup and configuration management.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/diagnostic_results.json'>diagnostic_results.json</a></b></td>
        <td style="padding: 8px;">Provides comprehensive health overview of system components and operational status.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/requirements.txt'>requirements.txt</a></b></td>
        <td style="padding: 8px;">Project dependencies for building high-performance web service with real-time communication.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/verify_data.py'>verify_data.py</a></b></td>
        <td style="padding: 8px;">In-depth inspection of MongoDB collections for structure and data consistency validation.</td>
    </tr>
    <tr style="border-bottom: 1px solid #eee;">
        <td style="padding: 8px;"><b><a href='https://github.com/CaitlinCalder/BI-Dashboard-Streaming/blob/master/mongodb_kafka_streaming.py'>mongodb_kafka_streaming.py</a></b></td>
        <td style="padding: 8px;">Core streaming pipeline for real-time data synchronization and enrichment from MongoDB to Kafka.</td>
    </tr>
</table>
</div>
</details>

---

## Getting Started

### Prerequisites

This project requires the following dependencies:

- **Programming Language:** Python 3.8+
- **Package Manager:** Pip
- **Container Runtime:** Docker and Docker Compose

### Installation

Build BI-Dashboard-Streaming from source and install dependencies:

1. **Clone the repository:**

    ```bash
    git clone https://github.com/CaitlinCalder/BI-Dashboard-Streaming
    ```

2. **Navigate to the project directory:**

    ```bash
    cd BI-Dashboard-Streaming
    ```

3. **Install the dependencies:**

**Using Docker:**
```bash
docker build -t CaitlinCalder/BI-Dashboard-Streaming .
```

**Using pip:**
```bash
pip install -r requirements.txt
```

### Usage

Run the project with:

**Using Docker:**
```bash
docker-compose up --build
```

**Using pip:**
```bash
python server.py
```

### Testing

Run the test suite with:

**Using Docker:**
```bash
docker-compose run app pytest
```

**Using pip:**
```bash
pytest
```

---

## License

BI-Dashboard-Streaming is protected under the MIT License. For more details, refer to the LICENSE file.

---

<div align="center">
<a href="#top">Return to Top</a>
</div>
