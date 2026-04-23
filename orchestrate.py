"""
Fenix Pipeline Orchestrator

This script handles the end-to-end execution of the Fenix data pipeline,
including ingestion, Spark processing, dbt modeling, and validation.
"""

import argparse
import subprocess
import sys
import time
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

from config.settings import REDPANDA_BROKER, BRONZE_PATH, PROJECT_ROOT


# --- Infrastructure Checks ---

def check_redpanda() -> bool:
    """Verify that the Redpanda broker is reachable."""
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": REDPANDA_BROKER})
        metadata = admin.list_topics(timeout=5)
        logger.info(f"  [OK] Redpanda connected. Topics: {list(metadata.topics.keys())}")
        return True
    except Exception as e:
        logger.error(f"  [FAIL] Redpanda not reachable: {e}")
        logger.error("  -> Ensure Redpanda is running: docker-compose up -d")
        return False


def create_topics():
    """Ensure required Kafka topics exist."""
    from confluent_kafka.admin import AdminClient, NewTopic
    from config.settings import TOPIC_MATCHES, TOPIC_STANDINGS

    admin = AdminClient({"bootstrap.servers": REDPANDA_BROKER})
    existing = admin.list_topics(timeout=5).topics.keys()

    topics_to_create = []
    for topic_name in [TOPIC_MATCHES, TOPIC_STANDINGS]:
        if topic_name not in existing:
            topics_to_create.append(
                NewTopic(topic_name, num_partitions=1, replication_factor=1)
            )

    if topics_to_create:
        futures = admin.create_topics(topics_to_create)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"  [OK] Created topic: {topic}")
            except Exception as e:
                logger.warning(f"  [WARN] Topic {topic}: {e}")
    else:
        logger.info("  [OK] All required topics exist")


# --- Pipeline Phases ---

def run_ingestion(include_historical: bool = False):
    """Execute the ingestion phase: Fetch from APIs and produce to Redpanda."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE: INGESTION")
    logger.info("=" * 60)

    from ingestion.producer import main as producer_main
    from ingestion.consumer import main as consumer_main

    producer_main(include_historical=include_historical)
    consumer_main(timeout=15)


def run_processing():
    """Execute the Spark ETL phase: Bronze -> Silver -> Gold."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE: SPARK PROCESSING")
    logger.info("=" * 60)

    import glob
    match_files = glob.glob(str(BRONZE_PATH / "matches" / "date=*" / "*.json"))
    standings_files = glob.glob(str(BRONZE_PATH / "standings" / "date=*" / "*.json"))

    if not match_files and not standings_files:
        logger.warning("  [WARN] No Bronze data found. Run ingestion first.")
        return

    logger.info(f"  Found {len(match_files)} match files, {len(standings_files)} standings files")

    from catalog.iceberg_catalog import setup_catalog
    setup_catalog()

    # Bronze -> Silver (Cleaning & Schema enforcement)
    from spark.bronze_to_silver import main as b2s_main
    b2s_main()

    # Silver -> Gold (Initial metrics materialization)
    from spark.silver_to_gold import main as s2g_main
    s2g_main()


def run_dbt():
    """Execute the dbt modeling phase: Silver -> Gold transformations."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE: DBT MODELS")
    logger.info("=" * 60)

    dbt_dir = PROJECT_ROOT / "fenix_dbt"
    from config.settings import LAKEHOUSE_PATH
    warehouse_abs = str(LAKEHOUSE_PATH).replace("\\", "/")

    logger.info("  Building dbt models...")
    result = subprocess.run(
        ["dbt", "run", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir),
         "--vars", f'{{"warehouse_path": "{warehouse_abs}"}}'],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT), shell=True
    )
    print(result.stdout)
    if result.returncode != 0:
        logger.error(f"  [FAIL] dbt run failed:\n{result.stderr}")
        return
    logger.info("  [OK] dbt models built successfully")

    logger.info("  Running dbt tests...")
    result = subprocess.run(
        ["dbt", "test", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir),
         "--vars", f'{{"warehouse_path": "{warehouse_abs}"}}'],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT), shell=True
    )
    print(result.stdout)
    if result.returncode != 0:
        logger.warning(f"  [WARN] dbt tests had failures:\n{result.stderr}")
    else:
        logger.info("  [OK] All dbt tests passed")


def run_validation():
    """Verify data quality across Silver and Gold layers."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE: VALIDATION")
    logger.info("=" * 60)

    from analytics.query_engine import validate_silver, validate_gold, get_connection

    conn = get_connection()
    silver_ok = validate_silver(conn)
    gold_ok = validate_gold(conn)
    conn.close()

    if silver_ok and gold_ok:
        logger.info("  [OK] Data quality validation successful")
    else:
        logger.warning("  [WARN] Data quality validation issues detected")


def run_demo():
    """Execute the analytics demo queries."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE: DEMO")
    logger.info("=" * 60)

    from analytics.query_engine import demo
    demo()


# --- Execution Entry Point ---

def main():
    parser = argparse.ArgumentParser(
        description="Fenix - Football Data Lakehouse Orchestrator"
    )
    parser.add_argument("--full", action="store_true",
                        help="Run entire pipeline (Ingest -> Process -> dbt -> Validate -> Demo)")
    parser.add_argument("--ingest-only", action="store_true", help="Only run ingestion")
    parser.add_argument("--process-only", action="store_true", help="Only run Spark processing")
    parser.add_argument("--dbt-only", action="store_true", help="Only run dbt models")
    parser.add_argument("--demo-only", action="store_true", help="Only run analytics demo")
    parser.add_argument("--historical", action="store_true", help="Include historical backfill data")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt transformation phase")

    args = parser.parse_args()

    print("\n")
    print("*" * 40)
    print("    FENIX - Football Data Lakehouse")
    print("*" * 40)
    print()

    start = time.time()

    if args.full:
        if not check_redpanda():
            sys.exit(1)
        create_topics()

        run_ingestion(include_historical=args.historical)
        run_processing()
        if not args.skip_dbt:
            run_dbt()
        run_validation()
        run_demo()

    elif args.ingest_only:
        if not check_redpanda():
            sys.exit(1)
        create_topics()
        run_ingestion(include_historical=args.historical)

    elif args.process_only:
        run_processing()

    elif args.dbt_only:
        run_dbt()

    elif args.demo_only:
        run_demo()

    else:
        parser.print_help()
        sys.exit(0)

    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"  * Pipeline completed in {elapsed:.1f}s")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
