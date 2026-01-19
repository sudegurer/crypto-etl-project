# Crypto Market Data ETL Pipeline

An end-to-end ETL pipeline to extract daily cryptocurrency market data from CoinGecko, clean & transform it, and load it into a PostgreSQL database for analytics-ready storage.  
Developed as a self-driven data engineering exercise to practice API ingestion, data transformation, and containerized deployments.

---

##  Project Overview

The pipeline follows the ETL (Extract → Transform → Load) pattern:

1. **Extract** – Fetch raw cryptocurrency data from the CoinGecko API  
2. **Transform** – Clean and standardize data for consistency  
3. **Load** – Insert transformed data into a PostgreSQL table

Goal: Convert raw external data into structured, query-ready tables for analytics and visualization.

---

##  Tech Stack

- **Python** – core ETL scripts  
- **Docker & Docker Compose** – containerized environment  
- **PostgreSQL** – relational database for analytics-ready storage  
- **Pandas & Requests** – data processing and API handling

---

##  Repository Structure
crypto-etl-project/
├── dags/                   # ETL scripts (Airflow-compatible)
├── data/                   # data storage (raw → clean)
├── logs/                   # logs
├── docker-compose.yml      # Docker setup
├── fetch_crypto_data.py    # Extract
├── transform_data.py       # Transform
├── load_data.py            # Load
├── README.md
---

##  How It Works

### 1. Extract (API Ingestion)
Fetch daily snapshots of top cryptocurrencies using CoinGecko API.  
Saved as `data/crypto_raw.csv`.

### 2. Transform (Cleaning & Standardization)
- Convert JSON → tabular format  
- Remove unnecessary columns  
- Clean data types (numerical, timestamps)  
- Standardize for DB ingestion  

Output: `data/crypto_clean.csv`.


### 3. Load (Database Insertion)
Insert cleaned data into PostgreSQL (`crypto_data` table).


---

##  Quick Start / Installation

> **Prerequisites:** Docker + Docker Compose installed

### 1) Clone the repository
```bash
git clone https://github.com/sudegurer/crypto-etl-project
cd crypto-etl-project
```
### 2) Start services
```bash
docker compose up -d
```
    
### 3) Run ETL steps (locally or using containers)

# Extract data
```bash
docker run --rm \
    --network crypto-etl-project_default \
    -v $(pwd)/dags:/opt/airflow/dags \
    -v $(pwd)/data:/opt/airflow/data \
    apache/airflow:2.8.0-python3.8 \
    python /opt/airflow/dags/fetch_crypto_data.py
```
# Transform data
```bash
docker run --rm \
    --network crypto-etl-project_default \
    -v $(pwd)/dags:/opt/airflow/dags \
    -v $(pwd)/data:/opt/airflow/data \
    apache/airflow:2.8.0-python3.8 \
    python /opt/airflow/dags/transform_data.py
```
# Load data
```bash
docker run --rm \
    --network crypto-etl-project_default \
    -v $(pwd)/dags:/opt/airflow/dags \
    -v $(pwd)/data:/opt/airflow/data \
    apache/airflow:2.8.0-python3.8 \
    python /opt/airflow/load_data.py
```
### Check the Database
# Find Postgres container ID
```bash
docker ps
```
# Connect to Postgres container
```bash
docker exec -it <POSTGRES_CONTAINER_ID> psql -U airflow -d airflow
```
In the Postgres prompt:
```bash
SELECT * FROM crypto_data LIMIT 5;
\q
```
⸻

 Database Overview

The database contains a single analytics-oriented table named crypto_data with columns like:
	•	coin_id
	•	symbol
	•	name
	•	current_price
	•	market_cap
	•	total_volume
	•	last_updated

This structure has been chosen to support simple analytical queries.

⸻

 What I Learned
	•	Designing a custom ETL pipeline
	•	Using Docker for reproducible environments
	•	API ingestion with proper transformation
	•	Analytics-ready data modeling

⸻

 Notes & Limitations
	•	Prototype for local development — scheduling, monitoring, or production automation isn’t implemented
	•	Rate limits / API quotas should be considered when scaling
	•	Production-ready setup would require orchestration (Airflow), monitoring, and logging

⸻

 Useful Links
	•	Converter repo: https://github.com/sudegurer/crypto-etl-project
