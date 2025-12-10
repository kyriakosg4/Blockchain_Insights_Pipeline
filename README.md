# Blockchain Insights Pipeline

## Overview

A fully automated ETL + analytics pipeline that collects real-time Ethereum blockchain data (blocks, transactions, and receipts) and market price data.
The system stores raw data in CSV files, loads into PostgreSQL, transforms using dbt, and performs daily data-consistency repair.

Initial part of the project involving about collecting and storing historical data before goes live.

This pipeline is designed to provide continuous on-chain analytics, updating every ~30 minutes for block data and every hour for price data.

---

## Features

**Live Data Collection**
- Fetches latest Ethereum blocks + transactions using Etherscan
- Fetches hourly Binance price candles
- Appends new rows to CSV and PostgreSQL
- Automatically continues from the last seen block

**Daily Gap Repair**  
Ensures no missing data by re-fetching:  
- all blocks for the previous day
- all hourly candles for the previous day

**Transformation with dbt**
- Cleans and enriches raw data
- Creates analytics tables (daily stats, APY, activity indicators)

**Automated Scheduling with Apache Airflow**  
Three DAGs orchestrate the system:
- ``live_etherscan_dag.py``
- ``live_binance_dag.py``
- ``daily_gap_repair.py``
- ``daily_dbt_build_dag.py``

---
## Project Structure

```nginx
Blockchain_Insights_Pipeline
│
├── dags/                   # Airflow DAG definitions
├── pipelines/              # ETL pipelines (live, historical, repair)
├── loader/                 # PostgreSQL loaders
├── clients/                # API clients (etherscan, binance)
├── chainpulse_dbt/         # dbt project for transformations
├── data/                   # CSV storage (created automatically)
└── analysis.ipynb          # exploratory analysis notebook
```
---

## Running the Pipeline with Docker + Airflow
This project ships with a minimal Airflow Docker setup so anyone can run it locally.

1️⃣ Install Docker Desktop

Download (Windows AMD64 version):  
``https://www.docker.com/products/docker-desktop/``

**Make sure virtualization / WSL2 is enabled.

2️⃣ Create an Airflow folder locally
Create a folder anywhere on your machine:
```javascript
C:/Users/<you>/airflow_docker/
```
Inside create:
```bash
dags/
plugins/
logs/
docker-compose.yaml
.env
```

Copy your DAG files into the dags/ folder:
```bash
live_etherscan_dag.py  
live_binance_dag.py  
daily_gap_repair.py  
daily_dbt_build_dag.py
```

3️⃣ Start Airflow

Open a terminal inside the airflow_docker folder and run:
```nginx
docker compose up -d
```
Then initialize the database:
```arduino
docker compose run airflow-webserver airflow db migrate
```
Create an admin user:
```css
docker compose run airflow-webserver airflow users create \
  --username admin \
  --firstname admin \
  --lastname user \
  --role Admin \
  --email admin@example.com
```

4️⃣ Open the Airflow UI

Go to:
```arduino
http://localhost:8080
```

Enable the DAGs → They will now start automatically:
- Live pipelines append data every 30–60 minutes
- Daily repair runs at 00:05
- dbt transformations run at 00:10

---

## Environment Variables

Your .env file contains secrets used by the pipelines:
```ini
ETHERSCAN_API_KEY=xxxx
DATABASE_URL=postgresql://...
```
