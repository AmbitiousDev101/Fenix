# 🦅 Fenix: Premier League Data Lakehouse

[![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Iceberg](https://img.shields.io/badge/Iceberg-1.7.0-005C84?logo=apache-iceberg&logoColor=white)](https://iceberg.apache.org/)
[![Redpanda](https://img.shields.io/badge/Redpanda-Kafka--Compatible-EF4444?logo=redpanda&logoColor=white)](https://redpanda.com/)
[![dbt](https://img.shields.io/badge/dbt-1.9.1-FF694B?logo=dbt&logoColor=white)](https://getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1.3-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)

Fenix is a production-grade data engineering project that demonstrates how to build a high-performance Data Lakehouse on a local machine. By combining the scalability of Apache Iceberg with the speed of DuckDB, this project processes live Premier League football data into actionable insights while maintaining strict ACID transactions and historical reproducibility.

## Project Overview

The core of Fenix is a hybrid ETL/ELT architecture following the Medallion pattern. It handles the full data lifecycle: from event-driven ingestion with Redpanda and Kafka, to complex data modeling with PySpark and dbt.

### Technical Achievements

**Event Driven Ingestion and Rate Limiting**
I engineered a resilient ingestion pipeline that fetches live match and standings data from multiple APIs. To handle strict API rate limits during historical backfills, I implemented a multithreaded token-bucket rate limiter. This mechanism successfully prevented over 500 potential HTTP 429 errors per hour, ensuring a seamless data flow even under high load.

**Lakehouse Architecture with Apache Iceberg**
Data is processed through Bronze, Silver, and Gold layers using memory-optimized PySpark workflows. By leveraging Apache Iceberg as the table format, the project supports features typically reserved for cloud warehouses, such as time travel snapshots and schema evolution, all running locally.

**High Performance Analytics with dbt and DuckDB**
The transformation layer uses dbt and DuckDB to materialize six core analytical models. This setup allows for sub-second query performance on the Streamlit dashboard, providing deep-dives into team form, league standings, and historical head-to-head statistics.

## Tech Stack

The architecture is built on a modern, industry-standard stack:
*   **Ingestion:** Python, Redpanda (Kafka), Requests
*   **Storage:** Apache Iceberg (Hadoop Catalog), Parquet
*   **Processing:** PySpark (Spark 3.5.4)
*   **Modeling:** dbt-duckdb, SQL
*   **Query Engine:** DuckDB
*   **Orchestration:** Apache Airflow
*   **Visualization:** Streamlit

## Getting Started

### Prerequisites
To run this project, you will need Python 3.11+, Java 17 (for PySpark), and Docker Desktop (for the Redpanda/Airflow infrastructure). You will also need a free API key from football-data.org.

### Setup and Execution
1. Clone the repository and install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set up your environment variables:
   ```bash
   cp .env.example .env
   # Add your FOOTBALL_DATA_API_KEY to the .env file
   ```
3. Launch the infrastructure and run the pipeline:
   ```bash
   docker-compose up -d
   python orchestrate.py --full
   ```
4. View the results on the dashboard:
   ```bash
   streamlit run dashboard.py
   ```

## Analytics Capabilities
The final Gold layer provides several key analytical models:
*   **Team Form:** Calculates rolling averages and performance trends for the last 5 matches.
*   **League Standings:** Computes live standings with tie-breaker logic.
*   **Head-to-Head:** Aggregates historical rivalry data between any two teams.
*   **Snapshot Exploration:** Demonstrates Iceberg's time-travel capability by querying the lakehouse as it existed at previous timestamps.

## License
This project is licensed under the MIT License.
