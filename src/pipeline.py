# ============================================================
# src/pipeline.py — End-to-end pipeline orchestrator
# Simulates: Apache Airflow DAG with task dependencies
#
# Task flow (mirrors Airflow DAG structure):
#   ingest_task >> transform_task >> load_task >> validate_task
# ============================================================

import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """
    Executes the full ETL pipeline in sequence.

    Stage 1: Ingestion   — Fetch from OpenWeatherMap API → save raw JSON
    Stage 2: Transform   — Parse, clean, detect anomalies, aggregate
    Stage 3: DB Load     — Insert into SQLite (fact + dimension tables)
    Stage 4: Validate    — Print summary to confirm data landed correctly
    """
    pipeline_start = time.time()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"🚀 PIPELINE RUN STARTED  |  run_id: {run_id}")
    logger.info("=" * 60)

    # ── STAGE 1: INGESTION ────────────────────────────────────
    logger.info("\n📥 STAGE 1: Data Ingestion")
    logger.info("-" * 40)
    t0 = time.time()

    try:
        from src.ingestion import run_ingestion
        saved_files = run_ingestion()

        if not saved_files:
            logger.error("❌ No data ingested — aborting pipeline")
            return False

        logger.info(f"✅ Stage 1 complete in {round(time.time()-t0, 2)}s | {len(saved_files)} files saved")

    except Exception as e:
        logger.error(f"❌ Stage 1 FAILED: {e}")
        return False

    # ── STAGE 2: TRANSFORMATION ───────────────────────────────
    logger.info("\n🔄 STAGE 2: Transformation & Anomaly Detection")
    logger.info("-" * 40)
    t0 = time.time()

    try:
        from src.transformation import run_transformation
        records_df, agg_df = run_transformation()

        if records_df.empty:
            logger.error("❌ No records after transformation — aborting")
            return False

        logger.info(f"✅ Stage 2 complete in {round(time.time()-t0, 2)}s | {len(records_df)} records processed")

    except Exception as e:
        logger.error(f"❌ Stage 2 FAILED: {e}")
        return False

    # ── STAGE 3: DATABASE LOAD ────────────────────────────────
    logger.info("\n🗄️  STAGE 3: Database Load")
    logger.info("-" * 40)
    t0 = time.time()

    try:
        from src.db_loader import run_db_load
        run_db_load(records_df, agg_df)
        logger.info(f"✅ Stage 3 complete in {round(time.time()-t0, 2)}s")

    except Exception as e:
        logger.error(f"❌ Stage 3 FAILED: {e}")
        return False

    # ── STAGE 4: VALIDATION ───────────────────────────────────
    logger.info("\n✅ STAGE 4: Post-Load Validation")
    logger.info("-" * 40)

    try:
        from src.db_loader import query_summary
        summary = query_summary()
        logger.info("📊 Data Summary:\n" + summary.to_string())

    except Exception as e:
        logger.warning(f"⚠️  Validation query failed (non-critical): {e}")

    # ── DONE ──────────────────────────────────────────────────
    total_duration = round(time.time() - pipeline_start, 2)
    logger.info("\n" + "=" * 60)
    logger.info(f"🎉 PIPELINE COMPLETE  |  Total time: {total_duration}s")
    logger.info("=" * 60)

    return True


if __name__ == "__main__":
    run_pipeline()
