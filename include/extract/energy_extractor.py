# 🎫 TICKET: IIOT-002

# Epic: Phase 1 - Foundation(Extract & Load)
# Title: Extract External Energy Market Data(SMARD.de API)
# Assignee: Data Engineer

# User Story: As a Data Engineer, I need a Python script that fetches historical wholesale electricity prices for Germany so that we can eventually join this data with our machine power consumption to calculate variable energy costs.

# Technical Context: We will use the official German electricity market platform, SMARD.de. They provide a public, free REST API. You will need the requests library for this.

# Acceptance Criteria(Definition of Done):

# [X] A new Python script(e.g., energy_extractor.py) is created.

# [X] The script uses the requests library to call the SMARD.de API.

# [X] The script fetches hourly wholesale electricity prices matching the timeframe of our telemetry data(First week of January 2026).

# [X] The script parses the response and maps it to a clean list of dictionaries with at least two keys: timestamp(converted from Unix milliseconds to a UTC ISO 8601 string) and price_eur_per_mwh(float).

# [X] The script includes basic error handling(e.g., checking for a 200 HTTP status code before processing).

# [X] The script saves the parsed data locally as raw_energy_prices_batch_1.json.

import requests
import datetime as dt
import json
from pathlib import Path

filter_id = 4169
region = "DE"
resolution = "hour"
timestamps_url = f"https://www.smard.de/app/chart_data/{filter_id}/{region}/index_{resolution}.json"

response = requests.get(timestamps_url)
if response.status_code == 200:
    # List of timestamps since 2018-10-01 00:00:00 to 2026-02-23 00:00:00
    initial_time_stamps = response.json()['timestamps']
    target_date = dt.datetime(
        2026, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc).timestamp() * 1000
    last_date = dt.datetime(
        2026, 1, 7, 23, 59, 59, tzinfo=dt.timezone.utc).timestamp() * 1000
    # List of dates that are smaller than the target date
    valid_data = [
        ts
        for ts in initial_time_stamps
        if ts <= target_date
    ]

    # Closest timestamp to the target
    nearest_time_stamp = int(max(valid_data))

    # Retrieves a list of timestamps: [[hourly_timestamps, price], etc..]
    data_url = f"https://www.smard.de/app/chart_data/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{nearest_time_stamp}.json"

    data_response = requests.get(data_url)
    if data_response.status_code == 200:
        data_response_json = data_response.json()['series']
    else:
        print(f"Error: {data_response.status_code}")

    # Filters for timestamps starting from 1/1/2026 00:00:00
    data_response_json_filtered = []
    for ts in data_response_json:
        if ts[0] >= target_date and ts[0] <= last_date:
            data_response_json_filtered.append(ts)

    hourly_prices_dict = [
        {
            'timestamp': str(dt.datetime.fromtimestamp(ts[0]/1000, tz=dt.timezone.utc).isoformat()),
            'price_eur_per_mwh': ts[1]
        }
        for ts in data_response_json_filtered
    ]

    # Checks if a the directory exists and creates it if it's not.
    file_path = Path('data/raw/raw_energy_prices_batch_1.json')

    # Create the parent directories (data and raw) if they don't exist
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the file
    with open(file_path, 'w') as f:
        json.dump(hourly_prices_dict, f, indent=4)

else:
    print(f"Error: {response.status_code}")
