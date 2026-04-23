"""
Fenix - Bronze to Silver ETL
Reads raw Bronze JSON files, flattens nested API structures from 2 different
sources into a unified schema, normalizes/deduplicates, and writes to Iceberg
Silver tables.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from spark.spark_session import get_spark_session
from config.settings import BRONZE_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# - Flatteners ----------------------------------------------------------------

def _flatten_football_data(df: DataFrame) -> DataFrame:
    """Flatten football-data.org nested JSON into unified Silver schema."""
    return df.select(
        F.col("id").cast("string").alias("match_id"),
        F.coalesce(
            F.col("_metadata.competition_id"),
            F.col("competition.code"),
            F.lit("PL")
        ).alias("competition_id"),
        F.to_timestamp(F.col("utcDate")).alias("utc_date"),
        F.lower(F.col("status")).alias("status"),
        F.col("matchday").cast("int").alias("matchday"),
        F.coalesce(F.col("stage"), F.lit("REGULAR_SEASON")).alias("stage"),
        F.col("homeTeam.id").cast("string").alias("home_team_id"),
        F.col("homeTeam.name").alias("home_team_name"),
        F.col("awayTeam.id").cast("string").alias("away_team_id"),
        F.col("awayTeam.name").alias("away_team_name"),
        F.col("score.fullTime.home").cast("int").alias("home_score"),
        F.col("score.fullTime.away").cast("int").alias("away_score"),
        F.to_timestamp(F.col("_metadata.ingested_at")).alias("ingested_at"),
        F.lit("football-data.org").alias("source"),
    )


def _flatten_thesportsdb(df: DataFrame) -> DataFrame:
    """Flatten thesportsdb.com flat JSON into unified Silver schema."""
    # Normalize status strings
    status_normalized = (
        F.when(F.lower(F.col("strStatus")).contains("finished"), F.lit("finished"))
         .when(F.lower(F.col("strStatus")).contains("not started"), F.lit("scheduled"))
         .when(F.lower(F.col("strStatus")).contains("postponed"), F.lit("postponed"))
         .otherwise(F.lower(F.col("strStatus")))
    )

    return df.select(
        F.col("idEvent").cast("string").alias("match_id"),
        F.coalesce(F.col("_metadata.competition_id"), F.lit("PL")).alias("competition_id"),
        F.to_timestamp(
            F.concat_ws(" ", F.col("dateEvent"), F.col("strTime"))
        ).alias("utc_date"),
        status_normalized.alias("status"),
        F.col("intRound").cast("int").alias("matchday"),
        F.lit("REGULAR_SEASON").alias("stage"),
        F.col("idHomeTeam").cast("string").alias("home_team_id"),
        F.col("strHomeTeam").alias("home_team_name"),
        F.col("idAwayTeam").cast("string").alias("away_team_id"),
        F.col("strAwayTeam").alias("away_team_name"),
        F.col("intHomeScore").cast("int").alias("home_score"),
        F.col("intAwayScore").cast("int").alias("away_score"),
        F.to_timestamp(F.col("_metadata.ingested_at")).alias("ingested_at"),
        F.lit("thesportsdb.com").alias("source"),
    )


# - Quality & Deduplication ---------------------------------------------------

def _add_quality_flags(df: DataFrame) -> DataFrame:
    """Add a quality_flag column: 'valid' if all required fields are non-null."""
    required_cols = ["match_id", "home_team_id", "away_team_id", "utc_date", "status"]
    all_non_null = F.lit(True)
    for col_name in required_cols:
        all_non_null = all_non_null & F.col(col_name).isNotNull()

    return df.withColumn(
        "quality_flag",
        F.when(all_non_null, F.lit("valid")).otherwise(F.lit("incomplete"))
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    """Deduplicate by match_id, keeping the most recently ingested record."""
    window = Window.partitionBy("match_id").orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


# - Process Matches -----------------------------------------------------------

def process_matches():
    """Read Bronze match JSON files, flatten, deduplicate, write to Silver Iceberg."""
    spark = get_spark_session()
    logger.info("Processing Bronze -> Silver (matches)...")

    # Read all Bronze match JSON files
    bronze_matches_path = BRONZE_PATH / "matches"
    json_glob = str(bronze_matches_path / "date=*" / "*.json")

    # Check if any files exist
    import glob
    files = glob.glob(json_glob)
    if not files:
        logger.warning("  [WARN] No Bronze match files found. Skipping.")
        return

    logger.info(f"  Reading {len(files)} Bronze file(s)...")
    raw_df = spark.read.option("multiLine", "true").json(
        [str(f) for f in [Path(p) for p in files]]
    )

    if raw_df.limit(1).count() == 0:
        logger.warning("  [WARN] Bronze match data is empty. Skipping.")
        return

    # Determine source and flatten accordingly
    columns = set(raw_df.columns)
    dfs_to_union = []

    # football-data.org data has "homeTeam" nested struct
    if "homeTeam" in columns:
        fd_df = raw_df.filter(
            F.col("_metadata.source") == "football-data.org"
        )
        if fd_df.limit(1).count() > 0:
            dfs_to_union.append(_flatten_football_data(fd_df))
            logger.info("  [OK] Flattened football-data.org matches")

    # thesportsdb.com data has "strHomeTeam" flat field
    if "strHomeTeam" in columns:
        tsdb_df = raw_df.filter(
            F.col("_metadata.source") == "thesportsdb.com"
        )
        if tsdb_df.limit(1).count() > 0:
            dfs_to_union.append(_flatten_thesportsdb(tsdb_df))
            logger.info("  [OK] Flattened thesportsdb.com matches")

    # If homeTeam not in columns but we still have data (might be mixed)
    if not dfs_to_union:
        # Try to flatten whatever we have as football-data format
        try:
            dfs_to_union.append(_flatten_football_data(raw_df))
            logger.info("  [OK] Flattened matches (auto-detected format)")
        except Exception as e:
            logger.error(f"  [FAIL] Could not flatten matches: {e}")
            return

    # Union all sources
    if len(dfs_to_union) == 1:
        unified_df = dfs_to_union[0]
    else:
        unified_df = dfs_to_union[0]
        for extra_df in dfs_to_union[1:]:
            unified_df = unified_df.unionByName(extra_df)

    # Add quality flags and deduplicate
    unified_df = _add_quality_flags(unified_df)
    unified_df = _deduplicate(unified_df)

    row_count = unified_df.count()
    logger.info(f"  -> Silver matches: {row_count} rows after dedup")

    # Write to Iceberg Silver
    unified_df.writeTo("fenix.silver.matches").using("iceberg").createOrReplace()
    logger.info("  [OK] Written to fenix.silver.matches")

    # Log latest snapshot
    try:
        spark.sql(
            "SELECT snapshot_id, committed_at FROM fenix.silver.matches.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).show(truncate=False)
    except Exception:
        pass


# - Process Standings ---------------------------------------------------------

def process_standings():
    """Read Bronze standings JSON, flatten, write to Silver Iceberg."""
    spark = get_spark_session()
    logger.info("Processing Bronze -> Silver (standings)...")

    bronze_standings_path = BRONZE_PATH / "standings"
    json_glob = str(bronze_standings_path / "date=*" / "*.json")

    import glob
    files = glob.glob(json_glob)
    if not files:
        logger.warning("  [WARN] No Bronze standings files found. Skipping.")
        return

    logger.info(f"  Reading {len(files)} Bronze file(s)...")
    raw_df = spark.read.option("multiLine", "true").json(
        [str(f) for f in [Path(p) for p in files]]
    )

    if raw_df.limit(1).count() == 0:
        logger.warning("  [WARN] Bronze standings data is empty. Skipping.")
        return

    # football-data.org standings are deeply nested:
    # standings[] -> each has type + table[] -> each row is a team
    try:
        standings_df = (
            raw_df
            .select(
                F.explode(F.col("standings")).alias("standing"),
                F.col("_metadata.ingested_at").alias("ingested_at"),
            )
            .filter(F.col("standing.type") == "TOTAL")
            .select(
                F.explode(F.col("standing.table")).alias("team_row"),
                F.col("ingested_at"),
            )
            .select(
                F.col("team_row.team.id").cast("string").alias("team_id"),
                F.col("team_row.team.name").alias("team_name"),
                F.col("team_row.position").cast("int").alias("position"),
                F.col("team_row.playedGames").cast("int").alias("played_games"),
                F.col("team_row.won").cast("int").alias("won"),
                F.col("team_row.draw").cast("int").alias("draw"),
                F.col("team_row.lost").cast("int").alias("lost"),
                F.col("team_row.points").cast("int").alias("points"),
                F.col("team_row.goalsFor").cast("int").alias("goals_for"),
                F.col("team_row.goalsAgainst").cast("int").alias("goals_against"),
                F.col("team_row.goalDifference").cast("int").alias("goal_difference"),
                F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),
            )
        )
    except Exception as e:
        logger.error(f"  [FAIL] Could not flatten standings: {e}")
        return

    # Deduplicate by team_id, keep latest
    window = Window.partitionBy("team_id").orderBy(F.col("ingested_at").desc())
    standings_df = (
        standings_df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    row_count = standings_df.count()
    logger.info(f"  -> Silver standings: {row_count} rows")

    standings_df.writeTo("fenix.silver.standings").using("iceberg").createOrReplace()
    logger.info("  [OK] Written to fenix.silver.standings")

    try:
        spark.sql(
            "SELECT snapshot_id, committed_at FROM fenix.silver.standings.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).show(truncate=False)
    except Exception:
        pass


# - Main ----------------------------------------------------------------------

def main():
    """Run Bronze -> Silver for both matches and standings."""
    logger.info("=" * 60)
    logger.info("FENIX SPARK - Bronze -> Silver")
    logger.info("=" * 60)
    process_matches()
    process_standings()
    logger.info("Bronze -> Silver complete [OK]")


if __name__ == "__main__":
    main()
