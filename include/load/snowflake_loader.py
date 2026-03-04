# 🎫 TICKET: IIOT-003
# Epic: Phase 1 - Foundation(Extract & Load)
# Title: Load Raw Data into Snowflake
# Assignee: Data Engineer

# User Story: As a Data Engineer, I need a Python script that connects to our Snowflake data warehouse and uploads the local JSON batches into a raw staging area,
# so that our data is centralized and ready for future SQL transformations.

# Acceptance Criteria(Definition of Done):

# [X] The snowflake-connector-python and python-dotenv packages are installed in your virtual environment.

# [X] A .env file is created to store your Snowflake credentials(Account, User, Password, Warehouse, Database, Schema) AND is added to your .gitignore.

# [X] In the Snowflake Web UI, a database(e.g., IIOT_FACTORY), a schema(e.g., RAW), and two tables(raw_telemetry and raw_energy) are created. Each table should have just one column: record VARIANT.

# [X] A new Python script(e.g., snowflake_loader.py) is written.

# [X] The script loads the credentials from the .env file, connects to Snowflake, reads the two local JSON files, and successfully INSERTs the data into the corresponding tables.


from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import errors
import os
from pathlib import Path


def setup_infrastructure(cur, snowflake_database, snowflake_schema):
    """Creates the necessary databases, schemas, and tables."""
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {snowflake_database};")
    cur.execute(f"USE DATABASE {snowflake_database};")

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_schema};")
    cur.execute(f"USE SCHEMA {snowflake_schema};")

    cur.execute("CREATE TABLE IF NOT EXISTS raw_telemetry(record VARIANT);")
    cur.execute("CREATE TABLE IF NOT EXISTS raw_energy(record VARIANT);")

    cur.execute("TRUNCATE TABLE raw_telemetry;")
    cur.execute("TRUNCATE TABLE raw_energy;")


def load_data(cur, table_name, file_path):
    """Uploads a local file to a table stage and copies it into the table."""
    try:
        cur.execute(
            f"PUT 'file://{file_path.as_posix()}' @%{table_name};")
        cur.execute(
            f"COPY INTO {table_name} FILE_FORMAT = (TYPE = 'JSON', STRIP_OUTER_ARRAY = TRUE);")
    except errors.Error as e:
        print(f"FAILED to load {table_name}. Error: {e}")


def main():
    current_dir = Path.cwd()
    file_path_telemetry = current_dir / "data" / "raw" / "raw_telemetry_batch_1.json"
    file_path_energy = current_dir / "data" / \
        "raw" / "raw_energy_prices_batch_1.json"

    if not file_path_telemetry.exists():
        raise FileNotFoundError(
            f"Telemetry data missing at: {file_path_telemetry}")
    if not file_path_energy.exists():
        raise FileNotFoundError(f"Energy data missing at: {file_path_energy}")

    # Loads env variables from the .env file
    load_dotenv(override=True)
    snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
    snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
    snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')

    try:
        conn = snowflake.connector.connect(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
        )

        with conn.cursor() as cur:
            setup_infrastructure(cur, snowflake_database, snowflake_schema)
            load_data(cur, 'raw_telemetry', file_path_telemetry)
            load_data(cur, 'raw_energy', file_path_energy)

    except errors.Error as e:
        print(f"Critical Connection Error: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
