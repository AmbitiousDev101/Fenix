import logging
from spark.spark_session import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def setup_catalog():
    spark = get_spark_session()
    for ns in ["silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS fenix.{ns}")
        logger.info(f"  [OK] Namespace fenix.{ns} ready")

    logger.info("\n-- Current Namespaces --")
    spark.sql("SHOW NAMESPACES IN fenix").show()

def list_tables():
    spark = get_spark_session()
    for ns in ["silver", "gold"]:
        spark.sql(f"SHOW TABLES IN fenix.{ns}").show()

if __name__ == "__main__":
    setup_catalog()
    list_tables()
