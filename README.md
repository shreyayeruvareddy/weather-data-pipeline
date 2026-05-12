# Automated Cloud Weather Data Pipeline

> End-to-end ETL pipeline ingesting live weather data from REST APIs, processing and validating records with anomaly detection, and loading into a structured database — orchestrated on a scheduled interval.

---

## Project Overview

This project simulates a **production-grade data engineering pipeline** built with the same architecture used in cloud-based data platforms. It ingests real-time weather data from the OpenWeatherMap API across 5 U.S. cities, transforms and validates the data, flags statistical anomalies using z-score analysis, and loads structured records into a relational database following a **star schema design**.

The local implementation mirrors a cloud deployment where:
| Local (this repo) | Production equivalent |
|---|---|
| `data/raw/` folder | AWS S3 raw zone |
| `data/processed/` folder | AWS S3 processed zone |
| SQLite database | PostgreSQL / AWS RDS |
| `scheduler.py` | Apache Airflow DAG (`dags/`) |

Swapping to production requires **one-line changes** per component — all abstracted behind config and connection utilities.

---

## Architecture

```
OpenWeatherMap API
        │
        ▼
┌──────────────────┐
│  src/ingestion   │  → Fetch JSON for 5 cities
│  (retry logic)   │  → Save to data/raw/*.json
└──────────────────┘
        │
        ▼
┌──────────────────┐
│src/transformation│  → Parse nested JSON
│ + anomaly detect │  → Clean & validate fields
│  (z-score ≥ 2.0) │  → Detect anomalies
└──────────────────┘  → Compute city aggregates
        │              → Save to data/processed/*.csv
        ▼
┌──────────────────┐
│  src/db_loader   │  → Upsert dim_city, dim_weather_condition
│  (star schema)   │  → Insert fact_weather_readings
└──────────────────┘  → Insert agg_city_daily
        │              → Log to pipeline_run_log
        ▼
┌──────────────────┐
│   scheduler.py   │  → Runs pipeline every 60 min
│  (or Airflow DAG)│  → dags/weather_pipeline_dag.py
└──────────────────┘
        │
        ▼
  SQLite Database  →  Tableau / Power BI Dashboard
```

---

## Database Schema (Star Schema)

```
                    ┌─────────────────────────┐
                    │   fact_weather_readings  │
                    │─────────────────────────│
              ┌────►│ reading_id (PK)          │◄────┐
              │     │ city_id (FK)             │     │
              │     │ condition_id (FK)        │     │
              │     │ timestamp_utc            │     │
              │     │ temperature_f / _c       │     │
              │     │ humidity_pct             │     │
              │     │ wind_speed_mph           │     │
              │     │ anomaly_flag             │     │
              │     │ anomaly_reason           │     │
              │     └─────────────────────────┘     │
              │                                      │
  ┌───────────┴──┐                    ┌──────────────┴──────┐
  │   dim_city   │                    │ dim_weather_condition│
  │──────────────│                    │─────────────────────│
  │ city_id (PK) │                    │ condition_id (PK)   │
  │ city_name    │                    │ weather_main        │
  │ country      │                    │ weather_desc        │
  └──────────────┘                    └─────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Data Source | OpenWeatherMap REST API |
| Data Processing | Pandas, NumPy |
| Anomaly Detection | Z-score (NumPy / Pandas) |
| Orchestration | `scheduler.py` (local) / Apache Airflow (production) |
| Storage - Raw | Local filesystem (→ AWS S3 in production) |
| Storage - DB | SQLite (→ PostgreSQL / AWS RDS in production) |
| Visualization | Tableau / Power BI (CSV export from `agg_city_daily`) |
| Version Control | Git / GitHub |

---

## Project Structure

```
weather-data-pipeline/
│
├── dags/
│   └── weather_pipeline_dag.py     # Airflow DAG (production)
│
├── src/
│   ├── ingestion.py                # Stage 1: API fetch + retry logic
│   ├── transformation.py           # Stage 2: clean + anomaly detection
│   ├── db_loader.py                # Stage 3: star schema DB load
│   └── pipeline.py                 # Orchestrates all 3 stages
│
├── data/
│   ├── raw/                        # Raw JSON files (S3 raw zone)
│   └── processed/                  # Cleaned CSVs (S3 processed zone)
│
├── notebooks/
│   └── eda_and_validation.ipynb    # EDA + Tableau export
│
├── config.py                       # API key, cities, thresholds
├── scheduler.py                    # Run pipeline once or on interval
├── requirements.txt
└── README.md
```

---

## Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/weather-data-pipeline.git
cd weather-data-pipeline
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Add your API key
Edit `config.py`:
```python
API_KEY = "your_openweathermap_api_key_here"
```
Get a free key at: https://openweathermap.org/api (activates in ~10 min)

### 4. Run the pipeline once
```bash
python scheduler.py
```

### 5. Run on a schedule (every 60 minutes)
```bash
python scheduler.py --loop
```

### 6. Custom interval (e.g., every 30 minutes)
```bash
python scheduler.py --loop --interval 30
```

---

## Key Features

### Retry Logic
Each city fetch retries up to **3 times** before marking as failed — mirrors Apache Airflow's `retries=3` task parameter.

### Anomaly Detection
Z-score threshold of **2.0** applied to temperature, wind speed, and humidity. Records exceeding the threshold are flagged with reason codes and stored in the database for downstream alerting.

### Star Schema Design
Fact and dimension tables enable fast aggregation queries for BI dashboards — optimized for Tableau and Power BI connections.

### Pipeline Audit Log
Every run logs stage, status, record count, duration, and errors to `pipeline_run_log` — enabling monitoring and debugging without external tools.

### Production Upgrade Path
Swap to real cloud infrastructure with minimal changes:

**AWS S3:**
```python
# In ingestion.py / transformation.py, replace file writes with:
import boto3
s3 = boto3.client("s3")
s3.put_object(Bucket="weather-raw-zone", Key=filename, Body=json_data)
```

**PostgreSQL:**
```python
# In db_loader.py, replace get_connection() with:
import psycopg2
return psycopg2.connect(host="...", database="weather_db", user="...", password="...")
```

**Apache Airflow:**
```bash
# Use the existing DAG file directly:
cp dags/weather_pipeline_dag.py $AIRFLOW_HOME/dags/
airflow dags trigger weather_data_pipeline
```

---

## Dashboard (Tableau / Power BI)

After running the pipeline, export data for visualization:

```python
import sqlite3, pandas as pd
conn = sqlite3.connect("data/weather_pipeline.db")
df = pd.read_sql("SELECT * FROM agg_city_daily", conn)
df.to_csv("data/processed/tableau_export.csv", index=False)
```

**Dashboard KPIs tracked:**
- Average / Max / Min temperature by city (hourly trend)
- Humidity and wind speed comparison across cities
- Anomaly count per city per day
- Pipeline run success/failure rate over time

---

## Sample Output

```
2025-01-15 14:00:01 [INFO] 🚀 PIPELINE RUN STARTED  |  run_id: 20250115_140001
2025-01-15 14:00:01 [INFO] 📥 STAGE 1: Data Ingestion
2025-01-15 14:00:03 [INFO] ✅ Successfully fetched data for Charlotte
2025-01-15 14:00:04 [INFO] ✅ Successfully fetched data for New York
2025-01-15 14:00:05 [INFO] ✅ Successfully fetched data for Chicago
2025-01-15 14:00:05 [INFO] ✅ Stage 1 complete in 4.2s | 5 files saved
2025-01-15 14:00:05 [INFO] 🔄 STAGE 2: Transformation & Anomaly Detection
2025-01-15 14:00:05 [INFO] ✅ Cleaned dataframe: 5 valid records
2025-01-15 14:00:05 [WARNING] 🚨 1 anomaly detected (z-score > 2.0)
2025-01-15 14:00:05 [INFO] ✅ Stage 2 complete in 0.3s | 5 records processed
2025-01-15 14:00:05 [INFO] 🗄️  STAGE 3: Database Load
2025-01-15 14:00:05 [INFO] ✅ Inserted 5 records into fact_weather_readings
2025-01-15 14:00:05 [INFO] 🎉 PIPELINE COMPLETE  |  Total time: 4.8s
```

---

## Skills Demonstrated

- ETL pipeline design and implementation (Python, SQL)
- REST API integration with error handling and retry logic
- Data cleaning, validation, and quality checks
- Statistical anomaly detection (z-score analysis)
- Relational database design (star schema)
- Pipeline orchestration (scheduler → Airflow upgrade path)
- Cloud architecture simulation (local → AWS S3 / PostgreSQL)
- Data modeling for BI and dashboard reporting

---

## Author

**Yeruva Bala Shreya Reddy**  
M.S. Computer Science (Data Science) — UNC Charlotte  
[LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN) | [Email](mailto:yeruvabalashreyareddy@gmail.com)
