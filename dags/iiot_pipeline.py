from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'hazem',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'iiot_data_pipeline',
    default_args=default_args,
    description='End-to-End ELT Pipeline for IIOT-009',
    schedule=timedelta(days=1),  # Runs once a day
    catchup=False,
    tags=['iiot', 'elt'],
) as dag:

    # Start and End Placeholders
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Task 1 & 2: Extract Data (These will run in parallel)
    extract_telemetry = BashOperator(
        task_id='extract_telemetry',
        bash_command='mkdir -p /usr/local/airflow/include/data/raw && cd /usr/local/airflow/include && python scripts/simulator.py'
    )

    extract_energy = BashOperator(
        task_id='extract_energy',
        bash_command='mkdir -p /usr/local/airflow/include/data/raw && cd /usr/local/airflow/include && python scripts/energy_extractor.py'
    )

    # Task 3: Load Data to Snowflake
    load_snowflake = BashOperator(
        task_id='load_snowflake',
        bash_command='cd /usr/local/airflow/include && python scripts/snowflake_loader.py'
    )

    # Task 4 & 5: Run dbt Transformations and Tests
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --project-dir /usr/local/airflow/include/iiot_transformations --profiles-dir /usr/local/airflow/include/iiot_transformations'
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='dbt test --project-dir /usr/local/airflow/include/iiot_transformations --profiles-dir /usr/local/airflow/include/iiot_transformations'
    )

    # Define the Dependencies (The Graph)
    start_pipeline >> [extract_telemetry,
                       extract_energy] >> load_snowflake >> run_dbt_models >> run_dbt_tests >> end_pipeline
