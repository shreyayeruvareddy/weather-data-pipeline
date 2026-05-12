# ============================================================
# src/db_loader.py — Load processed data into SQLite database
# Simulates: PostgreSQL loading in production
# Swap: Change connection string in get_connection() for PostgreSQL
# ============================================================

import sqlite3
import pandas as pd
import logging
import os
from datetime import datetime
from config import DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_connection():
    """
    Returns SQLite connection.

    🔁 To swap to PostgreSQL (production):
        import psycopg2
        return psycopg2.connect(
            host="your-host", database="weather_db",
            user="your-user", password="your-password"
        )
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def create_tables():
    """
    Create database schema if not exists.
    Star schema design:
      - fact_weather_readings  (fact table)
      - dim_city               (dimension table)
      - dim_weather_condition  (dimension table)
      - agg_city_daily         (pre-aggregated for Tableau)
      - pipeline_run_log       (audit/monitoring table)
    """
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        -- Dimension: City
        CREATE TABLE IF NOT EXISTS dim_city (
            city_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name   TEXT NOT NULL UNIQUE,
            country     TEXT
        );

        -- Dimension: Weather Condition
        CREATE TABLE IF NOT EXISTS dim_weather_condition (
            condition_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            weather_main    TEXT NOT NULL,
            weather_desc    TEXT NOT NULL,
            UNIQUE(weather_main, weather_desc)
        );

        -- Fact Table: Weather Readings
        CREATE TABLE IF NOT EXISTS fact_weather_readings (
            reading_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id         INTEGER REFERENCES dim_city(city_id),
            condition_id    INTEGER REFERENCES dim_weather_condition(condition_id),
            timestamp_utc   TEXT NOT NULL,
            temperature_f   REAL,
            temperature_c   REAL,
            feels_like_f    REAL,
            temp_min_f      REAL,
            temp_max_f      REAL,
            humidity_pct    REAL,
            pressure_hpa    REAL,
            wind_speed_mph  REAL,
            wind_deg        REAL,
            visibility_m    REAL,
            cloudiness_pct  REAL,
            heat_index      REAL,
            anomaly_flag    INTEGER DEFAULT 0,
            anomaly_reason  TEXT,
            ingested_at     TEXT,
            source_file     TEXT
        );

        -- Aggregation Table: Daily City Summary (for dashboards)
        CREATE TABLE IF NOT EXISTS agg_city_daily (
            agg_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name       TEXT,
            agg_date        TEXT,
            avg_temp_f      REAL,
            max_temp_f      REAL,
            min_temp_f      REAL,
            avg_humidity    REAL,
            avg_wind_mph    REAL,
            anomaly_count   INTEGER,
            record_count    INTEGER,
            created_at      TEXT
        );

        -- Pipeline Run Log: Audit table for monitoring
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp   TEXT NOT NULL,
            stage           TEXT NOT NULL,
            status          TEXT NOT NULL,
            records_processed INTEGER DEFAULT 0,
            error_message   TEXT,
            duration_sec    REAL
        );
    """)

    conn.commit()
    conn.close()
    logger.info("✅ Database schema created/verified")


def upsert_dimensions(df: pd.DataFrame) -> tuple[dict, dict]:
    """
    Insert new cities and weather conditions into dimension tables.
    Returns lookup dicts: {city_name: city_id}, {(main, desc): condition_id}
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Upsert dim_city
    for _, row in df[["city", "country"]].drop_duplicates().iterrows():
        cursor.execute(
            "INSERT OR IGNORE INTO dim_city (city_name, country) VALUES (?, ?)",
            (row["city"], row["country"])
        )

    # Upsert dim_weather_condition
    for _, row in df[["weather_main", "weather_desc"]].drop_duplicates().iterrows():
        cursor.execute(
            "INSERT OR IGNORE INTO dim_weather_condition (weather_main, weather_desc) VALUES (?, ?)",
            (row["weather_main"], row["weather_desc"])
        )

    conn.commit()

    # Build lookup dicts
    city_map = {row[1]: row[0] for row in cursor.execute("SELECT city_id, city_name FROM dim_city")}
    cond_map = {(row[1], row[2]): row[0] for row in cursor.execute(
        "SELECT condition_id, weather_main, weather_desc FROM dim_weather_condition"
    )}

    conn.close()
    return city_map, cond_map


def load_fact_table(df: pd.DataFrame, city_map: dict, cond_map: dict) -> int:
    """
    Load cleaned records into fact_weather_readings.
    Returns number of rows inserted.
    """
    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        city_id     = city_map.get(row["city"])
        condition_id = cond_map.get((row["weather_main"], row["weather_desc"]))

        cursor.execute("""
            INSERT INTO fact_weather_readings (
                city_id, condition_id, timestamp_utc,
                temperature_f, temperature_c, feels_like_f, temp_min_f, temp_max_f,
                humidity_pct, pressure_hpa, wind_speed_mph, wind_deg,
                visibility_m, cloudiness_pct, heat_index,
                anomaly_flag, anomaly_reason, ingested_at, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            city_id, condition_id, row["timestamp_utc"],
            row["temperature_f"], row["temperature_c"], row["feels_like_f"],
            row["temp_min_f"], row["temp_max_f"], row["humidity_pct"],
            row["pressure_hpa"], row["wind_speed_mph"], row["wind_deg"],
            row["visibility_m"], row["cloudiness_pct"], row.get("heat_index"),
            int(row["anomaly_flag"]), row["anomaly_reason"],
            row["ingested_at"], row["source_file"]
        ))
        inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"✅ Inserted {inserted} records into fact_weather_readings")
    return inserted


def load_aggregates(agg_df: pd.DataFrame) -> int:
    """
    Load aggregated city metrics into agg_city_daily.
    """
    conn = get_connection()
    cursor = conn.cursor()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    now   = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    inserted = 0

    for _, row in agg_df.iterrows():
        cursor.execute("""
            INSERT INTO agg_city_daily (
                city_name, agg_date, avg_temp_f, max_temp_f, min_temp_f,
                avg_humidity, avg_wind_mph, anomaly_count, record_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["city"], today, row["avg_temp_f"], row["max_temp_f"], row["min_temp_f"],
            row["avg_humidity"], row["avg_wind_mph"], int(row["anomaly_count"]),
            int(row["record_count"]), now
        ))
        inserted += 1

    conn.commit()
    conn.close()
    logger.info(f"✅ Inserted {inserted} aggregate rows into agg_city_daily")
    return inserted


def log_pipeline_run(stage: str, status: str, records: int = 0,
                     error: str = None, duration: float = None):
    """
    Write audit log entry to pipeline_run_log.
    Used for monitoring and debugging.
    """
    conn = get_connection()
    conn.execute("""
        INSERT INTO pipeline_run_log (run_timestamp, stage, status, records_processed, error_message, duration_sec)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), stage, status, records, error, duration))
    conn.commit()
    conn.close()


def query_summary() -> pd.DataFrame:
    """
    Query a summary of stored data — useful for validation.
    """
    conn = get_connection()
    query = """
        SELECT
            c.city_name,
            COUNT(f.reading_id)     AS total_readings,
            ROUND(AVG(f.temperature_f), 1) AS avg_temp_f,
            ROUND(AVG(f.humidity_pct), 1)  AS avg_humidity,
            SUM(f.anomaly_flag)     AS total_anomalies,
            MAX(f.timestamp_utc)    AS last_reading
        FROM fact_weather_readings f
        JOIN dim_city c ON f.city_id = c.city_id
        GROUP BY c.city_name
        ORDER BY total_readings DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def run_db_load(records_df: pd.DataFrame, agg_df: pd.DataFrame):
    """
    Main DB loading function — called from pipeline orchestrator.
    """
    import time
    start = time.time()

    try:
        create_tables()
        city_map, cond_map = upsert_dimensions(records_df)
        n_records = load_fact_table(records_df, city_map, cond_map)
        load_aggregates(agg_df)
        duration = round(time.time() - start, 2)
        log_pipeline_run("db_load", "SUCCESS", n_records, duration=duration)
        logger.info(f"✅ DB load complete in {duration}s")

    except Exception as e:
        log_pipeline_run("db_load", "FAILED", error=str(e))
        logger.error(f"❌ DB load failed: {e}")
        raise


if __name__ == "__main__":
    # Quick validation query
    print(query_summary().to_string())
