# ============================================================
# config.example.py — Copy this to config.py and fill in values
# This file IS safe to commit. config.py is gitignored.
# ============================================================

API_KEY = "YOUR_OPENWEATHERMAP_API_KEY_HERE"

CITIES = [
    "Charlotte",
    "New York",
    "Chicago",
    "Los Angeles",
    "Houston"
]

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
UNITS = "imperial"

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"
DB_PATH = "data/weather_pipeline.db"

Z_SCORE_THRESHOLD = 2.0
FETCH_INTERVAL_MINUTES = 60
