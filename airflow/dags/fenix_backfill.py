"""
Fenix — Weekly Backfill DAG
Schedule: Every Sunday at midnight UTC
Pipeline: Historical fetch → Bronze → Spark B→S → Spark S→G → dbt → Validate → Log snapshots
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

FENIX_DIR = "/opt/airflow/fenix"
DBT_DIR = f"{FENIX_DIR}/fenix_dbt"

default_args = {
    "owner": "fenix",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fenix_backfill",
    description="Weekly full backfill and reprocessing",
    schedule_interval="0 0 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fenix", "backfill"],
) as dag:

    fetch_historical = BashOperator(
        task_id="fetch_historical",
        bash_command=f"cd {FENIX_DIR} && python -m ingestion.producer --historical",
    )

    land_bronze = BashOperator(
        task_id="land_bronze",
        bash_command=f"cd {FENIX_DIR} && python -m ingestion.consumer 60",
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=f"cd {FENIX_DIR} && python -m spark.bronze_to_silver",
    )

    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=f"cd {FENIX_DIR} && python -m spark.silver_to_gold",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --profiles-dir . && "
            "dbt test --profiles-dir ."
        ),
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=f"cd {FENIX_DIR} && python -m analytics.query_engine validate_gold",
    )

    log_iceberg_snapshots = BashOperator(
        task_id="log_iceberg_snapshots",
        bash_command=f"cd {FENIX_DIR} && python -m analytics.query_engine log_snapshots",
    )

    (
        fetch_historical
        >> land_bronze
        >> spark_bronze_to_silver
        >> spark_silver_to_gold
        >> run_dbt
        >> validate_gold
        >> log_iceberg_snapshots
    )
