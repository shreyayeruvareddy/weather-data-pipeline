# Automated Cloud Data Pipeline — AWS S3 + Airflow + PostgreSQL

> Fully automated cloud data pipeline ingesting live weather data from OpenWeatherMap REST API across 5 cities into AWS S3 using boto3, orchestrated with Apache Airflow DAGs, with z-score anomaly detection, PostgreSQL loading, and an interactive Tableau dashboard.

---

## Project Overview

This pipeline ingests real-time weather data for 5 cities (Charlotte, New York, Los Angeles, Houston, Chicago), applies transformation and anomaly detection, loads into a structured PostgreSQL database, and exports Tableau-ready CSVs for visualization.

---

## Architecture

```
OpenWeatherMap REST API (5 cities)
        |
        v
[ Stage 1: Ingest   ]  src/ingestion.py    → AWS S3 raw zone via boto3
        |
        v
[ Stage 2: Transform]  src/transformation.py → JSON parsing, z-score anomaly detection
        |
        v
[ Stage 3: DB Load  ]  src/db_loader.py    → PostgreSQL / SQLite
        |
        v
[ Stage 4: Validate ]  Query summary       → Tableau CSV export
        |
        v
[ Orchestration     ]  dags/weather_dag.py → Apache Airflow DAG
```

---

## Key Features

- **REST API Integration**: Live weather data via OpenWeatherMap API (boto3 + requests)
- **AWS S3 Storage**: Raw/processed zone architecture with boto3 bucket management
- **Apache Airflow**: DAG-based orchestration with scheduling, retry logic, task dependencies
- **Anomaly Detection**: Z-score based statistical anomaly detection on temperature readings
- **Star Schema DB**: fact_weather_readings + dim_city + agg_city_daily tables
- **100% Pipeline Success Rate**: Error handling and retry mechanisms ensure reliable execution

---

## Live Results (July 27, 2026)

| City | Avg Temp (°F) | Avg Humidity | Anomalies |
|---|---|---|---|
| Houston | 82.29 | 68.0% | 0 |
| Charlotte | 74.26 | 62.5% | 0 |
| Chicago | 71.70 | 64.5% | 0 |
| New York | 71.23 | 47.0% | 0 |
| Los Angeles | 68.88 | 79.5% | 0 |

---

## 📊 Tableau Dashboard

**Live Dashboard:** [Weather Data Pipeline Dashboard](https://public.tableau.com/app/profile/bala.shreya.reddy.yeruva/viz/WeatherDataPipelineDashboard/WeatherDataPipelineDashboardYeruvaBalaShreyaReddy)

Dashboard includes:
- **Temperature by City** (bar chart) — Houston hottest at 82.29°F
- **Humidity by City** (horizontal bar) — Los Angeles most humid at 79.5%
- **Feels Like vs Actual Temp** (scatter plot) — temperature perception correlation
- **Wind Speed by City** (packed bubbles) — comparative wind visualization

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| API | OpenWeatherMap REST API |
| Cloud Storage | AWS S3 (boto3) |
| Orchestration | Apache Airflow (DAGs) |
| Database | PostgreSQL / SQLite |
| Anomaly Detection | Z-score (scipy/numpy) |
| Visualization | Tableau Dashboard |
| Version Control | Git / GitHub |

---

## Setup & Run

```bash
git clone https://github.com/shreyayeruvareddy/weather-data-pipeline.git
cd weather-data-pipeline
pip install -r requirements.txt
# Add your OpenWeatherMap API key to config.py
py -3.11 scheduler.py
```

---

## Author

**Yeruva Bala Shreya Reddy**
M.S. Computer Science (Data Science) — UNC Charlotte
[GitHub](https://github.com/shreyayeruvareddy) | [Email](mailto:yeruvabalashreyareddy@gmail.com)
