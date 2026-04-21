# Ad-Click-Data-Engineering-Pipeline
An end-to-end data engineering pipeline using Kafka, Spark, and Airflow to process 1M+ ad-click events with Delta Lake simulation and automated reporting.
Project Overview
This project is a comprehensive Data Engineering ecosystem designed to ingest, process, and monitor over 1,000,000 ad-click events. It simulates a high-scale production environment using Apache Kafka for real-time ingestion, Apache Spark for big data processing, and Apache Airflow for workflow orchestration.

The pipeline ensures data integrity through a custom Data Quality framework and utilizes a Delta Lake simulation to provide data versioning and "Time-Travel" capabilities.

Tech Stack
Language: Python 3.10

Data Processing: Apache Spark (Batch & Streaming)

Message Broker: Apache Kafka (KRaft mode)

Orchestration: Apache Airflow

Storage: SQLite (Warehouse) & Parquet (Lakehouse)

Quality Assurance: Pandas & NumPy Validation Framework

Architecture
Generation: Mock data generator creates 1M+ realistic ad-click records.

Ingestion: Data is published to Kafka topics to simulate live event streams.

Processing: Spark cleans raw data, handles schema enforcement, and performs star-schema transformations.

Orchestration: Airflow DAGs manage task dependencies, ensuring the pipeline only proceeds if data quality checks pass.

Monitoring: An automated system generates a standalone HTML dashboard showing key performance indicators (KPIs) like Total Clicks and Average Cost.

Key Features
Lakehouse Simulation: Implemented version control for datasets using Parquet metadata, allowing for historical data recovery.

Automated Data Quality: 3-sigma anomaly detection and completeness checks integrated directly into the pipeline.

End-to-End Automation: The entire stack, from raw ingestion to final reporting, is triggered via a single command or schedule.

Project Structure
/scripts: Core Python logic for generation, cleaning, and processing.

/data: Contains the SQL warehouse and the final dashboard.html report.

/s3_bucket: Simulated cloud storage for raw and processed assets.

ad_click_dag.py: The Airflow orchestration script.

How to Run
Start the Kafka broker and Airflow scheduler.

Place ad_click_dag.py in your Airflow DAGs folder.

Execute the master pipeline:

Bash
python3 scripts/final_pipeline.py
View the results in data/dashboard.html.

Author: Soumyajit Tarafdar

Roll Number: 23052926

Course: Data Engineering Capstone
