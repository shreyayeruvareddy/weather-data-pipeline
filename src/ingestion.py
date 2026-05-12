# ============================================================
# src/ingestion.py — Fetch raw weather data from OpenWeatherMap
# Simulates: AWS S3 raw zone ingestion via boto3
# ============================================================

import requests
import json
import os
import logging
from datetime import datetime
from config import API_KEY, BASE_URL, UNITS, CITIES, RAW_DATA_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def fetch_weather(city: str) -> dict | None:
    """
    Fetch current weather data for a given city from OpenWeatherMap API.
    Returns raw JSON response or None on failure.

    In production: response would be uploaded to AWS S3 raw zone using boto3.
    """
    params = {
        "q": city,
        "appid": API_KEY,
        "units": UNITS
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"✅ Successfully fetched data for {city}")
        return data

    except requests.exceptions.HTTPError as e:
        logger.error(f"❌ HTTP error for {city}: {e}")
    except requests.exceptions.ConnectionError:
        logger.error(f"❌ Connection error for {city} — check your internet")
    except requests.exceptions.Timeout:
        logger.error(f"❌ Request timed out for {city}")
    except Exception as e:
        logger.error(f"❌ Unexpected error for {city}: {e}")

    return None


def save_raw_data(city: str, data: dict) -> str:
    """
    Save raw JSON response to local raw zone.
    Simulates: boto3 s3.put_object() to S3 raw bucket.

    File naming: raw/{city}_{YYYYMMDD_HHMMSS}.json
    """
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{city.replace(' ', '_')}_{timestamp}.json"
    filepath = os.path.join(RAW_DATA_PATH, filename)

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"💾 Raw data saved → {filepath}")
    return filepath


def run_ingestion() -> list[str]:
    """
    Main ingestion function — loops over all configured cities,
    fetches data with retry logic, saves raw JSON files.

    Returns list of saved file paths.
    """
    saved_files = []
    failed_cities = []

    logger.info(f"🚀 Starting ingestion for {len(CITIES)} cities...")

    for city in CITIES:
        # Retry logic: attempt up to 3 times (simulates Airflow retry mechanism)
        for attempt in range(1, 4):
            data = fetch_weather(city)

            if data:
                filepath = save_raw_data(city, data)
                saved_files.append(filepath)
                break
            else:
                logger.warning(f"⚠️  Attempt {attempt}/3 failed for {city}")
                if attempt == 3:
                    failed_cities.append(city)
                    logger.error(f"🔴 All retries exhausted for {city} — skipping")

    logger.info(f"✅ Ingestion complete: {len(saved_files)} succeeded, {len(failed_cities)} failed")
    if failed_cities:
        logger.warning(f"Failed cities: {failed_cities}")

    return saved_files


if __name__ == "__main__":
    run_ingestion()
