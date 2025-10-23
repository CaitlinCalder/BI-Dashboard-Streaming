# BI-Dashboard-Streaming

<div align="center">

<img src="CLEARVUE.png" width="120" alt="BI Dashboard Streaming Logo"/>

# BI-Dashboard-Streaming

### Transform Data into Action in Real Time

<div style="margin: 20px 0;">

![License](https://img.shields.io/badge/License-MIT-1e40af?style=flat&logo=opensourceinitiative&logoColor=white)
![Last Commit](https://img.shields.io/github/last-commit/CaitlinCalder/BI-Dashboard-Streaming?style=flat&logo=git&logoColor=white&color=1e40af)
![Python](https://img.shields.io/badge/Python-3.8%2B-3776AB?style=flat&logo=python&logoColor=white)
![Version](https://img.shields.io/badge/Version-1.0.0-1e40af?style=flat)

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
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Testing](#testing)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Overview

**BI-Dashboard-Streaming** is a cutting-edge, enterprise-grade data pipeline solution that enables real-time data synchronization from MongoDB to Power BI dashboards through Apache Kafka. This system streamlines the entire process of capturing database changes, enriching data, and delivering live analytics within a scalable, resilient, and production-ready architecture.

### Why BI-Dashboard-Streaming?

This platform empowers data engineers and developers to build dynamic, real-time business intelligence systems with enterprise-level reliability and performance.

---

## Features

### Real-Time Data Streaming
Continuously captures MongoDB changes and streams updates to Kafka and Power BI with sub-second latency.

### Kafka Ecosystem Integration
Full orchestration of Kafka components including Zookeeper, Kafka Connect, and Kafka UI for scalable data pipelines.

### Advanced Diagnostics
Comprehensive tools to validate configurations, schemas, and data flow health across the entire pipeline.

### Centralized Configuration
Streamlined environment setup for MongoDB and Kafka connections with best-practice patterns.

### Automated Power BI Refresh
Seamless dataset updates ensuring your visualizations are always up-to-date.

### System Monitoring
Enterprise-grade health checks ensuring data pipeline integrity and reliability.

---

## Architecture

### System Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| Data Source | MongoDB Atlas | Primary database with change stream capabilities |
| Message Broker | Apache Kafka | High-throughput distributed streaming platform |
| API Layer | FastAPI | RESTful API for data access and control |
| Orchestration | Docker Compose | Container orchestration and service management |
| Visualization | Power BI | Real-time business intelligence dashboards |
| Data Validation | Pydantic | Schema validation and data integrity |

### Technical Specifications

| Aspect | Details |
|--------|---------|
| Architecture | Microservices-based streaming pipeline with decoupled components for maximum scalability |
| Code Quality | PEP 8 compliant Python with comprehensive Pydantic models for type safety |
| Documentation | Extensive inline documentation and comprehensive setup guides |
| Integrations | Native support for MongoDB, Kafka, Power BI, and WebSocket protocols |
| Modularity | Highly modular architecture with reusable components and clear separation of concerns |
| Testing | Schema validation and integration testing capabilities |
| Performance | Asynchronous processing with Kafka's high-throughput messaging (1M+ messages/sec) |
| Security | Environment-based secrets management with secure connection protocols |
| Dependencies | Minimal, well-maintained dependencies managed via requirements.txt |

---

## Getting Started

### Prerequisites

Ensure you have the following installed on your system:

| Requirement | Version | Purpose |
|-------------|---------|---------|
| Python | 3.8+ | Runtime environment |
| Docker | 20.10+ | Container runtime |
| Docker Compose | 1.29+ | Multi-container orchestration |
| Pip | Latest | Python package manager |

### Installation

Follow these steps to set up the project locally:

#### 1. Clone the Repository

```bash
git clone https://github.com/CaitlinCalder/BI-Dashboard-Streaming.git
cd BI-Dashboard-Streaming
```

#### 2. Environment Configuration

Create a `.env` file with your credentials:

```bash
MONGODB_URI=your_mongodb_connection_string
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POWERBI_WORKSPACE_ID=your_workspace_id
```

#### 3. Docker Setup

Build and launch all services:

```bash
docker-compose up -d
```

This will start:
- Zookeeper
- Kafka Broker
- Kafka Connect
- Kafka UI (accessible at http://localhost:8080)

#### 4. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Usage

**Starting the Streaming Pipeline**

```bash
python mongodb_kafka_streaming.py
```

**Launching the API Server**

```bash
python server.py
```

The FastAPI server will be available at http://localhost:8000 with interactive docs at http://localhost:8000/docs

**Running Diagnostics**

```bash
python verify_bi.py
python verify_data.py
```

### Testing

Execute the test suite to verify system integrity:

```bash
pytest tests/ -v
```

For comprehensive health checks:

```bash
python verify_bi.py
```

---

## Project Structure

```
BI-Dashboard-Streaming/
├── CMPG321_PowerBI.pbix          # Power BI dashboard template
├── ClearVueConfig.py             # Central configuration management
├── README.md                     # Project documentation
├── bi_dashboard.py               # Power BI integration module
├── clearvue_streaming.log        # Application logs
├── diagnostic_results.json       # System health diagnostics
├── docker-compose.yml            # Container orchestration config
├── kafka-plugins/                # Kafka connector plugins
│   └── mongo-kafka-connect-1.10.1-all.jar
├── kafka_message_schema.json     # Message schema definitions
├── mongodb_kafka_streaming.py    # Core streaming pipeline
├── requirements.txt              # Python dependencies
├── server.py                     # FastAPI application server
├── verify_bi.py                  # Power BI validation utility
└── verify_data.py                # Data validation utility
```

### Key Components

<details>
<summary><b>Core Modules</b></summary>

- `mongodb_kafka_streaming.py`: Main streaming pipeline orchestrating MongoDB change streams and Kafka producers
- `server.py`: FastAPI server providing RESTful endpoints and WebSocket connections
- `ClearVueConfig.py`: Centralized configuration for MongoDB and Kafka connections
- `bi_dashboard.py`: Power BI dataset refresh and authentication handling
</details>

<details>
<summary><b>Utilities</b></summary>

- `verify_bi.py`: Comprehensive Power BI integration diagnostics
- `verify_data.py`: MongoDB schema and data structure validation
- `kafka_message_schema.json`: JSON schema for Kafka message validation
</details>

<details>
<summary><b>Infrastructure</b></summary>

- `docker-compose.yml`: Multi-container Docker application setup
- `kafka-plugins/`: Kafka Connect MongoDB source connector
</details>

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## Contact

Project Maintainer: Caitlin Calder  
GitHub: [@CaitlinCalder](https://github.com/CaitlinCalder)  
Project Link: [https://github.com/CaitlinCalder/BI-Dashboard-Streaming](https://github.com/CaitlinCalder/BI-Dashboard-Streaming)

---

<div align="center">
<a href="#top">Back to Top</a>
</div>
