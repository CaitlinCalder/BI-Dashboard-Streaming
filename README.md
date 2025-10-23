<div id="top" align="center">

<!-- PROJECT LOGO -->
<img src="BI-Dashboard-Streaming.png" width="120" alt="BI Dashboard Streaming Logo"/>

# BI-DASHBOARD-STREAMING

<h3><em>Transform Data into Action in Real Time</em></h3>

<!-- BADGES -->
<p>
<img src="https://img.shields.io/github/license/CaitlinCalder/BI-Dashboard-Streaming?style=for-the-badge&logo=opensourceinitiative&logoColor=white&color=1e40af&labelColor=1e3a8a" alt="license">
<img src="https://img.shields.io/github/last-commit/CaitlinCalder/BI-Dashboard-Streaming?style=for-the-badge&logo=git&logoColor=white&color=1e40af&labelColor=1e3a8a" alt="last-commit">
<img src="https://img.shields.io/github/languages/top/CaitlinCalder/BI-Dashboard-Streaming?style=for-the-badge&color=1e40af&labelColor=1e3a8a" alt="repo-top-language">
<img src="https://img.shields.io/github/languages/count/CaitlinCalder/BI-Dashboard-Streaming?style=for-the-badge&color=1e40af&labelColor=1e3a8a" alt="repo-language-count">
</p>

<h4>Built with the tools and technologies:</h4>

<p>
<img src="https://img.shields.io/badge/Python-3776AB.svg?style=for-the-badge&logo=Python&logoColor=white" alt="Python">
<img src="https://img.shields.io/badge/FastAPI-009688.svg?style=for-the-badge&logo=FastAPI&logoColor=white" alt="FastAPI">
<img src="https://img.shields.io/badge/Docker-2496ED.svg?style=for-the-badge&logo=Docker&logoColor=white" alt="Docker">
<img src="https://img.shields.io/badge/MongoDB-47A248.svg?style=for-the-badge&logo=MongoDB&logoColor=white" alt="MongoDB">
<img src="https://img.shields.io/badge/Apache_Kafka-231F20.svg?style=for-the-badge&logo=Apache-Kafka&logoColor=white" alt="Kafka">
<img src="https://img.shields.io/badge/Pydantic-E92063.svg?style=for-the-badge&logo=Pydantic&logoColor=white" alt="Pydantic">
</p>

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Testing](#testing)
- [Project Structure](#-project-structure)
- [Contributing](#-contributing)
- [License](#-license)
- [Contact](#-contact)

---

## 🔷 Overview

**BI-Dashboard-Streaming** is a cutting-edge, enterprise-grade data pipeline solution that enables real-time data synchronization from MongoDB to Power BI dashboards through Apache Kafka. This system streamlines the entire process of capturing database changes, enriching data, and delivering live analytics within a scalable, resilient, and production-ready architecture.

### Why BI-Dashboard-Streaming?

This platform empowers data engineers and developers to build dynamic, real-time business intelligence systems with enterprise-level reliability and performance.

---

## ✨ Features

<table>
<tr>
<td>

### 🔄 Real-Time Data Streaming
Continuously captures MongoDB changes and streams updates to Kafka and Power BI with sub-second latency.

</td>
<td>

### 🚀 Kafka Ecosystem Integration
Full orchestration of Kafka components including Zookeeper, Kafka Connect, and Kafka UI for scalable data pipelines.

</td>
</tr>
<tr>
<td>

### 🔍 Advanced Diagnostics
Comprehensive tools to validate configurations, schemas, and data flow health across the entire pipeline.

</td>
<td>

### ⚙️ Centralized Configuration
Streamlined environment setup for MongoDB and Kafka connections with best-practice patterns.

</td>
</tr>
<tr>
<td>

### 📊 Automated Power BI Refresh
Seamless dataset updates ensuring your visualizations are always up-to-date.

</td>
<td>

### 📈 System Monitoring
Enterprise-grade health checks ensuring data pipeline integrity and reliability.

</td>
</tr>
</table>

---

## 🏗 Architecture

### System Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Source** | MongoDB Atlas | Primary database with change stream capabilities |
| **Message Broker** | Apache Kafka | High-throughput distributed streaming platform |
| **API Layer** | FastAPI | RESTful API for data access and control |
| **Orchestration** | Docker Compose | Container orchestration and service management |
| **Visualization** | Power BI | Real-time business intelligence dashboards |
| **Data Validation** | Pydantic | Schema validation and data integrity |

### Technical Specifications

|      | Aspect       | Details                                                                                     |
| :--- | :----------- | :------------------------------------------------------------------------------------------ |
| ⚙️  | **Architecture**  | Microservices-based streaming pipeline with decoupled components for maximum scalability |
| 🔩 | **Code Quality**  | PEP 8 compliant Python with comprehensive Pydantic models for type safety |
| 📄 | **Documentation** | Extensive inline documentation and comprehensive setup guides |
| 🔌 | **Integrations**  | Native support for MongoDB, Kafka, Power BI, and WebSocket protocols |
| 🧩 | **Modularity**    | Highly modular architecture with reusable components and clear separation of concerns |
| 🧪 | **Testing**       | Schema validation and integration testing capabilities |
| ⚡️  | **Performance**   | Asynchronous processing with Kafka's high-throughput messaging (1M+ messages/sec) |
| 🛡️ | **Security**      | Environment-based secrets management with secure connection protocols |
| 📦 | **Dependencies**  | Minimal, well-maintained dependencies managed via requirements.txt |

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed on your system:

| Requirement | Version | Purpose |
|------------|---------|---------|
| **Python** | 3.8+ | Runtime environment |
| **Docker** | 20.10+ | Container runtime |
| **Docker Compose** | 1.29+ | Multi-container orchestration |
| **Pip** | Latest | Python package manager |

### Installation

Follow these steps to set up the project locally:

#### 1️⃣ Clone the Repository

```bash
git clone https://github.com/CaitlinCalder/BI-Dashboard-Streaming.git
cd BI-Dashboard-Streaming
