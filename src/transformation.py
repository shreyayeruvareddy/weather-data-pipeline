# ============================================================
# src/transformation.py — Clean, transform, detect anomalies
# Simulates: S3 processed zone after transformation layer
# ============================================================

import json
import os
import glob
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from config import RAW_DATA_PATH, PROCESSED_DATA_PATH, Z_SCORE_THRESHOLD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def parse_raw_file(filepath: str) -> dict | None:
    """
    Parse a raw JSON file and extract relevant fields.
    Handles missing/null values gracefully.
    """
    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        record = {
            "city":           data.get("name", "Unknown"),
            "country":        data.get("sys", {}).get("country", "Unknown"),
            "timestamp_utc":  datetime.utcfromtimestamp(data.get("dt", 0)).strftime("%Y-%m-%d %H:%M:%S"),
            "temperature_f":  data.get("main", {}).get("temp"),
            "feels_like_f":   data.get("main", {}).get("feels_like"),
            "temp_min_f":     data.get("main", {}).get("temp_min"),
            "temp_max_f":     data.get("main", {}).get("temp_max"),
            "humidity_pct":   data.get("main", {}).get("humidity"),
            "pressure_hpa":   data.get("main", {}).get("pressure"),
            "wind_speed_mph": data.get("wind", {}).get("speed"),
            "wind_deg":       data.get("wind", {}).get("deg"),
            "visibility_m":   data.get("visibility"),
            "weather_main":   data.get("weather", [{}])[0].get("main", "Unknown"),
            "weather_desc":   data.get("weather", [{}])[0].get("description", "Unknown"),
            "cloudiness_pct": data.get("clouds", {}).get("all"),
            "sunrise_utc":    datetime.utcfromtimestamp(data.get("sys", {}).get("sunrise", 0)).strftime("%H:%M:%S"),
            "sunset_utc":     datetime.utcfromtimestamp(data.get("sys", {}).get("sunset", 0)).strftime("%H:%M:%S"),
            "source_file":    os.path.basename(filepath)
        }
        return record

    except Exception as e:
        logger.error(f"❌ Failed to parse {filepath}: {e}")
        return None


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data cleaning rules:
    - Drop rows missing critical fields
    - Fill optional nulls with defaults
    - Validate ranges (e.g., humidity must be 0–100)
    - Add derived columns
    """
    initial_count = len(df)

    # Drop rows missing critical temperature or city data
    df = df.dropna(subset=["temperature_f", "city", "timestamp_utc"])

    # Fill optional nulls
    df["wind_speed_mph"] = df["wind_speed_mph"].fillna(0.0)
    df["cloudiness_pct"] = df["cloudiness_pct"].fillna(0)
    df["visibility_m"]   = df["visibility_m"].fillna(10000)

    # Validate ranges
    df = df[df["humidity_pct"].between(0, 100, inclusive="both")]
    df = df[df["temperature_f"].between(-60, 140)]  # Reasonable F range

    # Derived columns
    df["temperature_c"] = ((df["temperature_f"] - 32) * 5 / 9).round(2)
    df["heat_index"] = df.apply(_compute_heat_index, axis=1)
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    dropped = initial_count - len(df)
    if dropped > 0:
        logger.warning(f"⚠️  Dropped {dropped} invalid records during cleaning")

    logger.info(f"✅ Cleaned dataframe: {len(df)} valid records")
    return df


def _compute_heat_index(row) -> float | None:
    """
    Compute heat index (feels-like) using NOAA formula.
    Only meaningful when temp >= 80°F and humidity >= 40%.
    """
    T = row["temperature_f"]
    H = row["humidity_pct"]
    if pd.isna(T) or pd.isna(H) or T < 80 or H < 40:
        return row["feels_like_f"]
    HI = (-42.379 + 2.04901523*T + 10.14333127*H
          - 0.22475541*T*H - 0.00683783*T**2
          - 0.05481717*H**2 + 0.00122874*T**2*H
          + 0.00085282*T*H**2 - 0.00000199*T**2*H**2)
    return round(HI, 2)


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Z-score based anomaly detection on temperature and wind speed.
    Flags records that deviate more than Z_SCORE_THRESHOLD standard deviations
    from the mean across all cities in the current batch.

    Threshold: Z_SCORE_THRESHOLD = 2.0 (configurable in config.py)
    """
    df = df.copy()
    df["anomaly_flag"] = False
    df["anomaly_reason"] = ""

    for col in ["temperature_f", "wind_speed_mph", "humidity_pct"]:
        if col not in df.columns or df[col].isna().all():
            continue

        mean = df[col].mean()
        std  = df[col].std()

        if std == 0:
            continue

        z_scores = (df[col] - mean) / std
        mask = z_scores.abs() > Z_SCORE_THRESHOLD

        df.loc[mask, "anomaly_flag"] = True
        df.loc[mask, "anomaly_reason"] += f"{col} z={z_scores[mask].round(2).astype(str)}; "

    anomaly_count = df["anomaly_flag"].sum()
    if anomaly_count > 0:
        logger.warning(f"🚨 {anomaly_count} anomalies detected (z-score > {Z_SCORE_THRESHOLD})")
        logger.warning(df[df["anomaly_flag"]][["city", "temperature_f", "wind_speed_mph", "anomaly_reason"]].to_string())
    else:
        logger.info("✅ No anomalies detected in this batch")

    return df


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute city-level aggregated metrics for the current batch.
    This is what gets visualized in Tableau dashboards.
    """
    agg = df.groupby("city").agg(
        avg_temp_f       = ("temperature_f", "mean"),
        max_temp_f       = ("temperature_f", "max"),
        min_temp_f       = ("temperature_f", "min"),
        avg_humidity     = ("humidity_pct", "mean"),
        avg_wind_mph     = ("wind_speed_mph", "mean"),
        anomaly_count    = ("anomaly_flag", "sum"),
        record_count     = ("city", "count")
    ).reset_index().round(2)

    logger.info(f"📊 Aggregates computed for {len(agg)} cities")
    return agg


def save_processed_data(df: pd.DataFrame, agg_df: pd.DataFrame) -> tuple[str, str]:
    """
    Save cleaned records and aggregates to processed zone CSVs.
    Simulates: boto3 s3.put_object() to S3 processed bucket.
    """
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    records_path = os.path.join(PROCESSED_DATA_PATH, f"weather_records_{timestamp}.csv")
    agg_path     = os.path.join(PROCESSED_DATA_PATH, f"weather_aggregates_{timestamp}.csv")

    df.to_csv(records_path, index=False)
    agg_df.to_csv(agg_path, index=False)

    logger.info(f"💾 Processed records → {records_path}")
    logger.info(f"💾 Aggregates        → {agg_path}")

    return records_path, agg_path


def run_transformation() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main transformation function:
    1. Load all raw JSON files from raw zone
    2. Parse → Clean → Detect anomalies → Aggregate
    3. Save to processed zone
    Returns (records_df, aggregates_df)
    """
    raw_files = glob.glob(os.path.join(RAW_DATA_PATH, "*.json"))

    if not raw_files:
        logger.warning("⚠️  No raw files found — run ingestion first")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"🔄 Transforming {len(raw_files)} raw files...")

    records = [parse_raw_file(f) for f in raw_files]
    records = [r for r in records if r is not None]

    df = pd.DataFrame(records)
    df = clean_dataframe(df)
    df = detect_anomalies(df)
    agg_df = compute_aggregates(df)

    save_processed_data(df, agg_df)

    return df, agg_df


if __name__ == "__main__":
    df, agg = run_transformation()
    print("\n--- Sample Records ---")
    print(df[["city", "temperature_f", "humidity_pct", "weather_desc", "anomaly_flag"]].to_string())
    print("\n--- Aggregates ---")
    print(agg.to_string())
