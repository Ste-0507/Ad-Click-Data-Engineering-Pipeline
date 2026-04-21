# Data Engineering Pipeline for Ad-Click Analytics

A scalable data engineering pipeline that ingests, processes, and monitors **1M+ ad-click events**, simulating a production-grade system.

---

## Overview

This project demonstrates an end-to-end data pipeline using:

- **Apache Kafka** → real-time ingestion  
- **Apache Spark** → distributed processing  
- **Apache Airflow** → workflow orchestration  

It includes a custom **data quality framework** and a **Delta Lake-style architecture** for versioning and time-travel queries.

---

## Tech Stack

| Layer              | Technology                     |
|--------------------|--------------------------------|
| Language           | Python 3.10                   |
| Processing         | Apache Spark (Batch/Streaming)|
| Messaging          | Apache Kafka (KRaft)          |
| Orchestration      | Apache Airflow                |
| Storage            | SQLite                        |
| File Format        | Parquet, CSV                  |
| Lakehouse Model    | Delta Lake (Simulation)       |

---

## Pipeline Architecture
[ Data Generator ]
↓
[ Kafka Topics ]
↓
[ Spark Processing ]
↓
[ Data Quality Checks ]
↓
[ SQLite Warehouse ]
↓
[ Dashboard Output ]


### Stages

**1. Data Generation**
- Produces 1M+ realistic ad-click records  

**2. Ingestion**
- Streams events into Kafka topics  

**3. Processing**
- Data cleaning and validation  
- Schema enforcement  
- Star schema transformation  

**4. Orchestration**
- Airflow DAG controls execution flow  
- Stops pipeline if quality checks fail  

**5. Monitoring**
- Generates HTML dashboard with:
  - Total Clicks  
  - Average Cost  

---

## Key Features

**Lakehouse Simulation**
- Version-controlled datasets using Parquet  
- Supports time-travel queries  

**Data Quality Framework**
- 3-sigma anomaly detection  
- Completeness checks  

**Automation**
- Entire pipeline runs with a single command  

---

## Project Structure
data_warehouse/ # Final SQL tables
delta_lake/ads/ # Versioned Parquet data
logs/ # Execution logs
s3_bucket/processed/ # Simulated cloud storage
scripts/ # Core pipeline code
README.md # Documentation
project_report.docx # Full report


---

## Setup

> Large datasets are excluded to keep repo size under 100MB.

### Install Dependencies

```bash
pip3 install pandas numpy pyspark kafka-python pyarrow

# Run Pipeline
python3 scripts/final_pipeline.py

# Verify Airflow DAG
export AIRFLOW_HOME=~/airflow_home
airflow dags test ad_click_automation_v1 2026-04-21
```
View Dashboard

Open in browser:
dashboard.html


Soumyajit Tarafdar
Roll Number: 23052926
Course: Data Engineering
