# TICKET: IIOT-001

# Epic: Phase 1 - Foundation (Extract & Load)
# Title: Build the Telemetry Data Simulator
# Assignee: Data Engineer

# User Story: As a Data Engineer, I need a Python script that generates realistic, randomized manufacturing telemetry data so that we have a mock data source to ingest into our data warehouse.

# Technical Context:
# We need to generate a list of JSON objects representing machine events. You will want to use standard Python libraries like uuid, datetime, random, and potentially json.

# Acceptance Criteria (Definition of Done):
# [X] A local Git repository is initialized for the project.
# [X] A Python virtual environment (venv) is created and activated.
# [X] A .gitignore file is created (ensure your venv folder is in it!).
# [X] A Python script (e.g., simulator.py) is written that generates a batch of exactly 100 machine telemetry records.
# [X] The generated records strictly match the JSON structure we agreed upon earlier (event_id, timestamp, machine_id, plant_id, operational_status, sensors, production).
# [X] The timestamps must be in UTC ISO 8601 format.
# [X] The script must successfully save the 100 records into a local file named raw_telemetry_batch_1.json.

import json
import uuid
import datetime as dt
from faker import Faker

fake = Faker()
num_rows = 100
machine_pool = [fake.unique.pystr_format(
    "ID-#######{{random_letter}}") for _ in range(10)]
plant_pool = [fake.unique.bothify(
    text='PLANT-??-##') for _ in range(5)]


simulator_dict = [
    {
        "event_id": str(uuid.uuid4()),
        'timestamp': fake.date_time_between_dates(
            datetime_start=dt.datetime(
                2026, 1, 1, tzinfo=dt.timezone.utc),
            datetime_end=dt.datetime(
                2026, 1, 7, tzinfo=dt.timezone.utc)
        ).isoformat(),
        'machine_id': fake.random_element(
            elements=machine_pool
        ),
        'plant_id': fake.random_element(
            elements=plant_pool
        ),
        'operational_status': fake.random_element(elements=['RUNNING', 'IDLE', 'MAINTENANCE', 'ERROR']),
        'sensors': {
            "temperature_celsius": round(fake.pyfloat(min_value=60, max_value=90), 1),
            "vibration_mm_s": round(fake.pyfloat(min_value=0.5, max_value=5.0), 1),
            "spindle_speed_rpm": fake.random_int(min=800, max=2500, step=50),
            "power_draw_kw": round(fake.pyfloat(min_value=10, max_value=25), 1)
        },
        'production': {
            "units_produced": fake.random_int(min=1, max=20),
            "defective_units": 1 if (fake.random_int(min=1, max=20) > 5 and fake.boolean(chance_of_getting_true=10)) else 0
        }
    }
    for i in range(num_rows)
]

with open('data/raw/raw_telemetry_batch_1.json', 'w') as f:
    json.dump(simulator_dict, f, indent=4)
