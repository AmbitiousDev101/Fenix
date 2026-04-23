"""
Fenix — Iceberg Time Travel Demo
Uses PySpark to demonstrate Iceberg's time travel capabilities:
snapshot listing, point-in-time queries, and snapshot comparison.
"""

import logging
from spark.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def list_snapshots(table_name: str):
    """List all snapshots for an Iceberg table."""
    spark = get_spark_session()
    print(f"\n  📸 Snapshots for {table_name}:")
    try:
        df = spark.sql(f"""
            SELECT snapshot_id, committed_at, operation,
                   summary['added-records'] AS added_records,
                   summary['total-records'] AS total_records
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
        """)
        df.show(truncate=False)
        return df.collect()
    except Exception as e:
        print(f"  (no snapshots: {e})")
        return []


def query_at_snapshot(table_name: str, snapshot_id: int):
    """Query an Iceberg table at a specific snapshot ID."""
    spark = get_spark_session()
    print(f"\n  🕐 Querying {table_name} at snapshot {snapshot_id}:")
    try:
        df = spark.read.option("snapshot-id", str(snapshot_id)).table(table_name)
        count = df.count()
        print(f"     Row count: {count}")
        df.show(5, truncate=False)
        return count
    except Exception as e:
        print(f"  Error: {e}")
        return 0


def compare_snapshots(table_name: str):
    """Compare the 2 most recent snapshots of an Iceberg table."""
    spark = get_spark_session()
    print(f"\n  🔄 Comparing snapshots for {table_name}:")

    try:
        snapshots = spark.sql(f"""
            SELECT snapshot_id, committed_at
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT 2
        """).collect()

        if len(snapshots) < 2:
            print("     Need at least 2 snapshots to compare. Run pipeline again!")
            return

        newest = snapshots[0]
        oldest = snapshots[1]

        print(f"     Newest: snapshot {newest['snapshot_id']} @ {newest['committed_at']}")
        print(f"     Oldest: snapshot {oldest['snapshot_id']} @ {oldest['committed_at']}")

        # Query both snapshots
        new_count = spark.read.option("snapshot-id", str(newest["snapshot_id"])).table(table_name).count()
        old_count = spark.read.option("snapshot-id", str(oldest["snapshot_id"])).table(table_name).count()

        diff = new_count - old_count
        direction = "+" if diff >= 0 else ""
        print(f"     Row count: {old_count} → {new_count} ({direction}{diff})")

    except Exception as e:
        print(f"  Error: {e}")


def main():
    """Run the full time travel demo across Gold tables."""
    print("\n")
    print("═" * 60)
    print("  📸 FENIX — Iceberg Time Travel Demo")
    print("═" * 60)

    gold_tables = [
        "fenix.gold.team_form",
        "fenix.gold.match_results",
    ]

    for table in gold_tables:
        print(f"\n{'─' * 60}")
        print(f"  TABLE: {table}")
        print(f"{'─' * 60}")

        snapshots = list_snapshots(table)

        if len(snapshots) >= 2:
            # Query oldest snapshot
            oldest_id = snapshots[-1]["snapshot_id"]
            print(f"\n  ⏪ Traveling back to oldest snapshot ({oldest_id}):")
            query_at_snapshot(table, oldest_id)

            # Compare newest vs oldest
            compare_snapshots(table)
        elif len(snapshots) == 1:
            print("  ℹ Only 1 snapshot. Run the pipeline again for time travel!")
        else:
            print("  ℹ No snapshots. Run the pipeline first!")

    # Also check Silver tables
    silver_tables = ["fenix.silver.matches", "fenix.silver.standings"]
    for table in silver_tables:
        snapshots = list_snapshots(table)

    print(f"\n{'═' * 60}")
    print("  Time Travel Demo Complete ✓")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
