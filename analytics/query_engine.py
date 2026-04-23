"""
Fenix — DuckDB Query Engine
Provides query functions for analytics, validation functions for Airflow tasks,
and a formatted demo() for showcasing the pipeline output.
"""

import sys
import logging

import duckdb
import pandas as pd
from tabulate import tabulate

from config.settings import DUCKDB_PATH, LAKEHOUSE_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with Iceberg + HTTPFS extensions loaded."""
    conn = duckdb.connect(str(DUCKDB_PATH))
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    return conn


def _iceberg_path(layer: str, table: str) -> str:
    """Build absolute path for iceberg_scan(), using forward slashes for DuckDB."""
    return str(LAKEHOUSE_PATH / layer / table).replace("\\", "/")


def _print_table(df: pd.DataFrame, title: str):
    """Pretty-print a pandas DataFrame with a title."""
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")
    if df.empty:
        print("  (no data)")
    else:
        print(tabulate(df, headers="keys", tablefmt="rounded_grid",
                       showindex=False, maxcolwidths=25))
    print()


# ── Query Functions ───────────────────────────────────────────────────────────

def get_team_form(conn: duckdb.DuckDBPyConnection,
                  team_name: str = None) -> pd.DataFrame:
    """Query Gold team_form table. Optionally filter by team name."""
    path = _iceberg_path("gold", "team_form")
    query = f"""
        SELECT team_name, utc_date, venue, goals_scored, goals_conceded,
               result, points, points_last5,
               goals_scored_rolling_avg_5 AS avg_gf,
               goals_conceded_rolling_avg_5 AS avg_gc,
               form_trend
        FROM iceberg_scan('{path}', allow_moved_paths = true)
    """
    if team_name:
        query += f" WHERE team_name ILIKE '%{team_name}%'"
    query += " ORDER BY utc_date DESC LIMIT 20"
    return conn.execute(query).fetchdf()


def get_league_standings(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Compute league standings from Gold team_form table."""
    path = _iceberg_path("gold", "team_form")
    query = f"""
        SELECT team_name,
               COUNT(*) AS played,
               SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS won,
               SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS drawn,
               SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS lost,
               SUM(points) AS pts,
               SUM(goals_scored) AS gf,
               SUM(goals_conceded) AS gc,
               SUM(goals_scored) - SUM(goals_conceded) AS gd,
               ROW_NUMBER() OVER (
                   ORDER BY SUM(points) DESC,
                            SUM(goals_scored) - SUM(goals_conceded) DESC,
                            SUM(goals_scored) DESC
               ) AS pos
        FROM iceberg_scan('{path}', allow_moved_paths = true)
        GROUP BY team_name
        ORDER BY pos
    """
    return conn.execute(query).fetchdf()


def get_head_to_head(conn: duckdb.DuckDBPyConnection,
                     team_a: str, team_b: str) -> pd.DataFrame:
    """Query Gold match_results for matches between two teams."""
    path = _iceberg_path("gold", "match_results")
    query = f"""
        SELECT utc_date, home_team_name, away_team_name,
               home_score, away_score, result
        FROM iceberg_scan('{path}', allow_moved_paths = true)
        WHERE (home_team_name ILIKE '%{team_a}%' AND away_team_name ILIKE '%{team_b}%')
           OR (home_team_name ILIKE '%{team_b}%' AND away_team_name ILIKE '%{team_a}%')
        ORDER BY utc_date DESC
    """
    return conn.execute(query).fetchdf()


# ── Validation Functions (called by Airflow) ─────────────────────────────────

def validate_silver(conn: duckdb.DuckDBPyConnection = None) -> bool:
    """Validate Silver data: rows > 0, no null match_ids, no negative scores."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    path = _iceberg_path("silver", "matches")
    try:
        result = conn.execute(f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN match_id IS NULL THEN 1 ELSE 0 END) AS null_match_ids,
                SUM(CASE WHEN home_score < 0 OR away_score < 0 THEN 1 ELSE 0 END) AS negative_scores
            FROM iceberg_scan('{path}', allow_moved_paths = true)
        """).fetchdf()

        total_rows = int(result["total_rows"].iloc[0])
        null_ids = int(result["null_match_ids"].iloc[0])
        neg_scores = int(result["negative_scores"].iloc[0])

        passed = total_rows > 0 and null_ids == 0 and neg_scores == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"  Silver validation {status}: {total_rows} rows, "
                     f"{null_ids} null IDs, {neg_scores} negative scores")
        return passed
    except Exception as e:
        logger.error(f"  ✗ Silver validation error: {e}")
        return False
    finally:
        if close_conn:
            conn.close()


def validate_gold(conn: duckdb.DuckDBPyConnection = None) -> bool:
    """Validate Gold data: valid form trends, valid points_last5 range."""
    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    path = _iceberg_path("gold", "team_form")
    try:
        result = conn.execute(f"""
            SELECT
                SUM(CASE WHEN form_trend NOT IN ('improving', 'declining') THEN 1 ELSE 0 END) AS invalid_trends,
                SUM(CASE WHEN points_last5 < 0 OR points_last5 > 15 THEN 1 ELSE 0 END) AS invalid_points
            FROM iceberg_scan('{path}', allow_moved_paths = true)
        """).fetchdf()

        invalid_trends = int(result["invalid_trends"].iloc[0])
        invalid_points = int(result["invalid_points"].iloc[0])

        passed = invalid_trends == 0 and invalid_points == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"  Gold validation {status}: {invalid_trends} invalid trends, "
                     f"{invalid_points} invalid points_last5")
        return passed
    except Exception as e:
        logger.error(f"  ✗ Gold validation error: {e}")
        return False
    finally:
        if close_conn:
            conn.close()


def log_iceberg_snapshots():
    """Log Iceberg snapshot history using PySpark."""
    from spark.spark_session import get_spark_session
    spark = get_spark_session()

    tables = [
        "fenix.silver.matches", "fenix.silver.standings",
        "fenix.gold.team_form", "fenix.gold.match_results",
    ]

    print(f"\n{'═' * 60}")
    print("  📸 ICEBERG SNAPSHOT HISTORY")
    print(f"{'═' * 60}")

    for table in tables:
        try:
            print(f"\n  Table: {table}")
            spark.sql(f"""
                SELECT snapshot_id, committed_at, operation,
                       summary['added-records'] AS added_records,
                       summary['total-records'] AS total_records
                FROM {table}.snapshots
                ORDER BY committed_at DESC
            """).show(truncate=False)
        except Exception as e:
            print(f"  (no snapshots yet: {e})")


# ── Demo ──────────────────────────────────────────────────────────────────────

def demo():
    """Full demo: query all analytics, validate, log snapshots."""
    print("\n")
    print("🦅" * 30)
    print("  FENIX — Football Data Lakehouse Demo")
    print("🦅" * 30)

    conn = get_connection()

    try:
        # League Standings
        standings = get_league_standings(conn)
        _print_table(standings, "📊 PREMIER LEAGUE STANDINGS")

        # Team Form (show top team if standings exist)
        if not standings.empty:
            top_team = standings.iloc[0]["team_name"]
            form = get_team_form(conn, top_team)
            _print_table(form, f"🔥 TEAM FORM: {top_team}")

        # Head to Head (Arsenal vs others if available)
        if len(standings) >= 2:
            t1 = standings.iloc[0]["team_name"]
            t2 = standings.iloc[1]["team_name"]
            h2h = get_head_to_head(conn, t1, t2)
            _print_table(h2h, f"⚔️ HEAD TO HEAD: {t1} vs {t2}")

        # Validations
        print(f"\n{'─' * 60}")
        print("  🔍 DATA QUALITY VALIDATION")
        print(f"{'─' * 60}")
        validate_silver(conn)
        validate_gold(conn)

    except Exception as e:
        logger.error(f"Demo error: {e}")
        print(f"\n  ⚠ Error: {e}")
        print("  → Run the pipeline first: python orchestrate.py --full")
    finally:
        conn.close()

    # Iceberg snapshots
    try:
        log_iceberg_snapshots()
    except Exception as e:
        logger.warning(f"Snapshot logging skipped: {e}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "demo"

    if cmd == "demo":
        demo()
    elif cmd == "validate_silver":
        conn = get_connection()
        ok = validate_silver(conn)
        conn.close()
        sys.exit(0 if ok else 1)
    elif cmd == "validate_gold":
        conn = get_connection()
        ok = validate_gold(conn)
        conn.close()
        sys.exit(0 if ok else 1)
    elif cmd == "log_snapshots":
        log_iceberg_snapshots()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python -m analytics.query_engine [demo|validate_silver|validate_gold|log_snapshots]")
        sys.exit(1)
