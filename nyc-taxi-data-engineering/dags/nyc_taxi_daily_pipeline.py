from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import duckdb

# ----------------------------
# CONFIG
# ----------------------------
DBT_PROJECT_DIR = "/workspaces/Firmable_technical_assessment/nyc-taxi-data-engineering"
DBT_PROFILES_DIR = os.path.expanduser("~/.dbt")
DUCKDB_PATH = f"{DBT_PROJECT_DIR}/data/nyc_taxi.duckdb"
DATA_PATH = f"{DBT_PROJECT_DIR}/data"

# ----------------------------
# DEFAULT ARGS
# ----------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

# ----------------------------
# DAG
# ----------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",  # 02:00 UTC
    catchup=True,  # enables backfill
    tags=["nyc_taxi", "dbt"],
) as dag:

    # ----------------------------
    # 1. CHECK SOURCE FRESHNESS
    # ----------------------------
    def check_source_freshness(**context):
        import datetime

        execution_date = context["ds"]  # YYYY-MM-DD
        date_obj = datetime.datetime.strptime(execution_date, "%Y-%m-%d")

        # Example file naming pattern (adjust if needed)
        file_pattern = f"{DATA_PATH}/yellow_tripdata_{date_obj.strftime('%Y-%m')}.parquet"

        if not os.path.exists(file_pattern):
            raise FileNotFoundError(f"Missing source file: {file_pattern}")

        print(f"Source file exists: {file_pattern}")

    check_source = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
        provide_context=True,
    )

    # ----------------------------
    # 2. RUN DBT STAGING
    # ----------------------------
    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select tag:staging --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ----------------------------
    # 3. RUN DBT INTERMEDIATE
    # ----------------------------
    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select tag:intermediate --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ----------------------------
    # 4. RUN DBT MARTS
    # ----------------------------
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select tag:mart --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ----------------------------
    # 5. RUN DBT TESTS
    # ----------------------------
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ----------------------------
    # 6. NOTIFY SUCCESS
    # ----------------------------
    def notify_success(**context):
        execution_date = context["ds"]

        con = duckdb.connect(DUCKDB_PATH)

        result = con.execute(f"""
            SELECT
                COUNT(*) AS total_trips,
                SUM(total_amount) AS total_revenue
            FROM fct_trips
            WHERE DATE(pickup_datetime) = '{execution_date}'
        """).fetchone()

        total_trips, total_revenue = result

        print("✅ Pipeline Success Summary")
        print(f"Date: {execution_date}")
        print(f"Total Trips: {total_trips}")
        print(f"Total Revenue: {total_revenue}")

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
    )

    # ----------------------------
    # DAG DEPENDENCIES
    # ----------------------------
    (
        check_source
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> notify
    )