from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = "/usr/local/airflow/include/dbt/dbt_clinical_data"
PROFILES_DIR = "/usr/local/airflow/include/dbt/profiles"

with DAG(
    dag_id="dbt_run_dag",
    start_date=datetime(2026, 4, 21),
    schedule=None,
    catchup=False,
    tags=["dbt", "snowflake"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_DIR} && dbt debug --profiles-dir {PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR}",
    )

    dbt_debug >> dbt_run >> dbt_test