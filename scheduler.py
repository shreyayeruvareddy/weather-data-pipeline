# ============================================================
# scheduler.py — Runs the pipeline on a schedule
# Simulates: Apache Airflow DAG with schedule_interval="@hourly"
#
# Usage:
#   python scheduler.py              # Runs once immediately
#   python scheduler.py --loop       # Runs every FETCH_INTERVAL_MINUTES
#   python scheduler.py --interval 30  # Custom interval in minutes
# ============================================================

import time
import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import FETCH_INTERVAL_MINUTES
from src.pipeline import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Weather Pipeline Scheduler")
    parser.add_argument("--loop",     action="store_true", help="Run continuously on interval")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_MINUTES,
                        help=f"Interval in minutes (default: {FETCH_INTERVAL_MINUTES})")
    args = parser.parse_args()

    if args.loop:
        logger.info(f"⏰ Scheduler started — running every {args.interval} minutes")
        logger.info("   Press Ctrl+C to stop\n")
        run_count = 0

        while True:
            run_count += 1
            logger.info(f"🔁 Scheduled run #{run_count}")
            success = run_pipeline()

            if success:
                logger.info(f"✅ Run #{run_count} succeeded")
            else:
                logger.error(f"❌ Run #{run_count} failed — will retry next interval")

            next_run = args.interval * 60
            logger.info(f"💤 Sleeping {args.interval} minutes until next run...\n")
            time.sleep(next_run)

    else:
        logger.info("▶️  Running pipeline once...")
        success = run_pipeline()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
