"""
Fenix - Silver to Gold ETL
Reads Silver Iceberg tables, computes analytics with PySpark window functions,
and writes to Gold Iceberg tables.
"""

import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from spark.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def build_team_form():
    """Build team form table with rolling averages."""
    spark = get_spark_session()
    logger.info("Building Gold team_form...")

    try:
        matches = spark.table("fenix.silver.matches").filter(F.col("status") == "finished")
    except Exception as e:
        logger.error(f"  [FAIL] Could not read Silver matches: {e}")
        return

    if matches.rdd.isEmpty():
        logger.warning("  [WARN] No finished matches in Silver. Skipping team_form.")
        return

    # Home perspective
    home_df = matches.select(
        "match_id", "competition_id", "utc_date",
        F.col("home_team_id").alias("team_id"),
        F.col("home_team_name").alias("team_name"),
        F.col("away_team_id").alias("opponent_id"),
        F.col("away_team_name").alias("opponent_name"),
        F.lit("home").alias("venue"),
        F.col("home_score").alias("goals_scored"),
        F.col("away_score").alias("goals_conceded")
    )

    # Away perspective
    away_df = matches.select(
        "match_id", "competition_id", "utc_date",
        F.col("away_team_id").alias("team_id"),
        F.col("away_team_name").alias("team_name"),
        F.col("home_team_id").alias("opponent_id"),
        F.col("home_team_name").alias("opponent_name"),
        F.lit("away").alias("venue"),
        F.col("away_score").alias("goals_scored"),
        F.col("home_score").alias("goals_conceded")
    )

    # Union perspectives
    form_df = home_df.unionByName(away_df)

    # Add result and points
    form_df = form_df.withColumn(
        "result",
        F.when(F.col("goals_scored") > F.col("goals_conceded"), "W")
         .when(F.col("goals_scored") == F.col("goals_conceded"), "D")
         .otherwise("L")
    ).withColumn(
        "points",
        F.when(F.col("result") == "W", 3)
         .when(F.col("result") == "D", 1)
         .otherwise(0)
    )

    # Window functions
    window_5 = Window.partitionBy("team_id").orderBy("utc_date").rowsBetween(-4, Window.currentRow)
    window_trend = Window.partitionBy("team_id").orderBy("utc_date")

    form_df = form_df.withColumn(
        "goals_scored_rolling_avg_5",
        F.round(F.avg("goals_scored").over(window_5), 2)
    ).withColumn(
        "goals_conceded_rolling_avg_5",
        F.round(F.avg("goals_conceded").over(window_5), 2)
    ).withColumn(
        "points_last5",
        F.sum("points").over(window_5)
    )

    # Form trend
    form_df = form_df.withColumn(
        "prev_points_last5",
        F.lag("points_last5", 5).over(window_trend)
    ).withColumn(
        "form_trend",
        F.when(F.col("prev_points_last5").isNull(), "improving")
         .when(F.col("points_last5") >= F.col("prev_points_last5"), "improving")
         .otherwise("declining")
    ).drop("prev_points_last5")

    row_count = form_df.count()
    logger.info(f"  -> Gold team_form: {row_count} rows")

    form_df.writeTo("fenix.gold.team_form").using("iceberg").createOrReplace()
    logger.info("  [OK] Written to fenix.gold.team_form")

    try:
        spark.sql(
            "SELECT snapshot_id, committed_at FROM fenix.gold.team_form.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).show(truncate=False)
    except Exception:
        pass


def build_match_results():
    """Build match results table."""
    spark = get_spark_session()
    logger.info("Building Gold match_results...")

    try:
        matches = spark.table("fenix.silver.matches").filter(F.col("status") == "finished")
    except Exception as e:
        logger.error(f"  [FAIL] Could not read Silver matches: {e}")
        return

    if matches.rdd.isEmpty():
        logger.warning("  [WARN] No finished matches in Silver. Skipping match_results.")
        return

    results_df = matches.withColumn(
        "result",
        F.when(F.col("home_score") > F.col("away_score"), "home_win")
         .when(F.col("home_score") < F.col("away_score"), "away_win")
         .otherwise("draw")
    ).select(
        "match_id", "competition_id", "utc_date", "matchday",
        "home_team_id", "home_team_name", "away_team_id", "away_team_name",
        "home_score", "away_score", "result", "source"
    )

    row_count = results_df.count()
    logger.info(f"  -> Gold match_results: {row_count} rows")

    results_df.writeTo("fenix.gold.match_results").using("iceberg").createOrReplace()
    logger.info("  [OK] Written to fenix.gold.match_results")

    try:
        spark.sql(
            "SELECT snapshot_id, committed_at FROM fenix.gold.match_results.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).show(truncate=False)
    except Exception:
        pass


def main():
    """Run Silver -> Gold for all tables."""
    logger.info("=" * 60)
    logger.info("FENIX SPARK - Silver -> Gold")
    logger.info("=" * 60)
    build_team_form()
    build_match_results()
    logger.info("Silver -> Gold complete [OK]")


if __name__ == "__main__":
    main()
