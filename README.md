# Industrial IoT & Telemetry Pipeline (Micro-Batch OEE)

This is a local ELT pipeline built to track how much money factory machines cost to run based on real-time German energy prices.

In manufacturing, Overall Equipment Effectiveness (OEE) is a standard metric, but running power-heavy machines during peak grid pricing can ruin profit margins. This project simulates high-frequency machine telemetry, ingests hourly Day-Ahead wholesale electricity prices from the SMARD.de API, and joins them in a Snowflake Star Schema to calculate the real-time financial impact of machine downtime.

## System Architecture

<details>
  <summary><b>Click to view the Architecture Diagram</b></summary>
  <img src="assets/architecture_diagram.png" width="800">
</details>

* **Data Generation & Ingestion (Python):** Custom simulators generate realistic factory JSON telemetry. The `requests` library fetches live wholesale energy prices via REST API.
* **Data Warehouse (Snowflake):** Acts as the single-source-of-truth data warehouse. Raw JSON is loaded directly into `VARIANT` columns via the `snowflake-connector-python`.
* **Transformation & Modeling (dbt Core):** Unpacks semi-structured JSON, enforces data quality tests, and models the data into a Kimball Star Schema (Fact and Dimension tables).
* **Orchestration (Apache Airflow):** Containerized using the Astro CLI, Airflow runs the pipeline as a daily micro-batch DAG, handling parallel extraction and sequential loading.

<details>
  <summary><b>Click to view the DAG</b></summary>
  <img src="assets/airflow_dag.png" width="800">
</details>

* **Visualization (Snowsight):** A custom dashboard tracking machine status distribution and continuous energy cost metrics.

<details>
  <summary><b>Click to view the snowsight dashboard</b></summary>
  <img src="assets/snowsight_dashboard.png" width="800">
</details>

## Technical Focus & Challenges Solved
* **Handling Semi-Structured Data:** Built robust dbt models to flatten nested JSON telemetry without losing data integrity.

* **Idempotent Pipelines:** Ensured Airflow tasks are fully idempotent, meaning the DAG can be rerun for past dates without creating duplicate records in Snowflake.

* **Data Quality:** Implemented automated dbt testing to catch missing telemetry or failed API responses before they reach the BI layer.

## Repository Structure

```text
Micro-Batch-OEE-Pipeline/
├── dags/                           # Airflow DAG definition (elt_pipeline_dag.py)
├── include/
│   ├── extract/                    # Python API and Simulator scripts
│   ├── load/                       # Snowflake ingestion scripts
│   └── iiot_transformations/       # dbt Core project (models, tests, schema.yml)
├── Dockerfile                      # Astro CLI image configuration
├── requirements.txt                # Python dependencies (dbt-snowflake, etc.)
└── README.md
```

## ⚙️ How to Run Locally
This project uses the Astronomer CLI to spin up a local Airflow environment via Docker.

### 1. Clone the repository:
```text
git clone https://github.com/your-hazemabollfadl/Micro-Batch-OEE-Pipeline.git
cd Micro-Batch-OEE-Pipeline
```

### 2. Configure your Snowflake Credentials:
Create a ```.env``` file in the root directory and add your Snowflake trial credentials:
```text
SNOWFLAKE_ACCOUNT=your_account_locator
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=IIOT_FACTORY
SNOWFLAKE_SCHEMA=RAW
```

### 3. Start the Airflow Cluster:
Ensure Docker is running (OrbStack recommended for Apple Silicon), then run:
```text
astro dev start
```

### 4. Trigger the Pipeline:
Navigate to http://localhost:8080 (default credentials: admin / admin). Unpause the iiot_data_pipeline DAG and trigger it manually to watch the ELT process execute!
