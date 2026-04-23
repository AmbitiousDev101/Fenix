import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# -- OS Environment Fixes --
if os.name == "nt":
    os.environ["HADOOP_HOME"] = str(Path(__file__).resolve().parent.parent / "hadoop")
    os.environ["PATH"] += os.pathsep + str(Path(__file__).resolve().parent.parent / "hadoop" / "bin")

# -- Project Paths --
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE_PATH = Path(os.getenv("LAKEHOUSE_PATH") or str(PROJECT_ROOT / "lakehouse"))
BRONZE_PATH = LAKEHOUSE_PATH / "bronze"
SILVER_PATH = LAKEHOUSE_PATH / "silver"
GOLD_PATH   = LAKEHOUSE_PATH / "gold"
DUCKDB_PATH = LAKEHOUSE_PATH / "fenix_analytics.duckdb"

# Auto-create directories on import
for p in [BRONZE_PATH / "matches", BRONZE_PATH / "standings", SILVER_PATH, GOLD_PATH]:
    p.mkdir(parents=True, exist_ok=True)

# -- DEV_MODE --
DEV_MODE = os.getenv("DEV_MODE", "true").lower() == "true"

# -- API Configuration --
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
FOOTBALL_DATA_BASE_URL = "https://api.football-data.org/v4"
THESPORTSDB_BASE_URL = "https://www.thesportsdb.com/api/v1/json/3"
PREMIER_LEAGUE_ID = "PL"
THESPORTSDB_PL_ID = "4328"

# Rate limiting
RATE_LIMIT_REQUESTS = 10  # tokens
RATE_LIMIT_PERIOD = 60    # seconds

# DEV_MODE date range (only last 7 days)
if DEV_MODE:
    DATE_FROM = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    DATE_TO = datetime.utcnow().strftime("%Y-%m-%d")
else:
    DATE_FROM = None
    DATE_TO = None

# -- Redpanda / Kafka --
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
TOPIC_MATCHES = "fenix-matches"
TOPIC_STANDINGS = "fenix-standings"
CONSUMER_GROUP = "fenix-bronze-writer"
CONSUMER_BATCH_SECONDS = 30

# -- Spark Configuration --
SPARK_CONFIG = {
    "spark.app.name": "Fenix",
    "spark.master": "local[2]",
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.sql.shuffle.partitions": "4",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.fenix": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.fenix.type": "hadoop",
    "spark.sql.catalog.fenix.warehouse": str(LAKEHOUSE_PATH),
    "spark.sql.defaultCatalog": "fenix",
    "spark.executorEnv.PYSPARK_PYTHON": "python.exe",
    "spark.executorEnv.PYSPARK_DRIVER_PYTHON": "python.exe",
}

# -- Competition Mapping --
COMPETITIONS = {
    "PL": {"name": "Premier League", "thesportsdb_id": "4328"},
}
