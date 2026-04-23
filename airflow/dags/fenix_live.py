"""
Fenix — Live Ingestion DAG
Schedule: Every 5 minutes, 6AM-11PM UTC
Pipeline: API health check → Producer → Consumer → Bronze check → Spark Bronze→Silver → Validate
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
import os
import glob
import time

FENIX_DIR = "/opt/airflow/fenix"

default_args = {
    "owner": "fenix",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def _check_new_bronze(**kwargs):
    """Check if any Bronze JSON files were modified in the last 10 minutes."""
    bronze_path = os.path.join(FENIX_DIR, "lakehouse", "bronze")
    cutoff = time.time() - 600  # 10 minutes ago
    for pattern in ["matches/date=*//*.json", "standings/date=*//*.json"]:
        full_pattern = os.path.join(bronze_path, pattern)
        for f in glob.glob(full_pattern, recursive=True):
            if os.path.getmtime(f) > cutoff:
                return True
    return False


with DAG(
    dag_id="fenix_live",
    description="Live Premier League data ingestion every 5 minutes",
    schedule_interval="*/5 6-23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fenix", "live"],
) as dag:

    check_api_health = BashOperator(
        task_id="check_api_health",
        bash_command=(
            'python -c "'
            "import requests; "
            "r = requests.get('https://api.football-data.org/v4/competitions/PL', "
            "headers={'X-Auth-Token': '${FOOTBALL_DATA_API_KEY}'}, timeout=10); "
            "r.raise_for_status(); "
            "print('API healthy:', r.status_code)"
            '"'
        ),
    )

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=f"cd {FENIX_DIR} && python -m ingestion.producer",
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=f"cd {FENIX_DIR} && python -m ingestion.consumer 30",
    )

    check_new_bronze = ShortCircuitOperator(
        task_id="check_new_bronze",
        python_callable=_check_new_bronze,
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=f"cd {FENIX_DIR} && python -m spark.bronze_to_silver",
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=f"cd {FENIX_DIR} && python -m analytics.query_engine validate_silver",
    )

    (
        check_api_health
        >> run_producer
        >> run_consumer
        >> check_new_bronze
        >> spark_bronze_to_silver
        >> validate_silver
    )
