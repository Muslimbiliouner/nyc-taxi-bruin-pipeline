"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: double
  - name: taxi_type
    type: string
  - name: payment_type_id
    type: integer
  - name: extracted_at
    type: timestamp
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime

def materialize():

    start_date = os.getenv("BRUIN_START_DATE")
    end_date = os.getenv("BRUIN_END_DATE")

    vars_json = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(vars_json)

    taxi_types = vars_dict.get("taxi_types", ["yellow", "green"])

    # Simulated ingestion (homework style)
    # In real life this would fetch remote files
    data = []

    for taxi in taxi_types:
        df = pd.DataFrame({
            "pickup_datetime": pd.date_range(start=start_date, periods=10, freq="H"),
            "dropoff_datetime": pd.date_range(start=start_date, periods=10, freq="H"),
            "pickup_location_id": [1]*10,
            "dropoff_location_id": [2]*10,
            "fare_amount": [20.0]*10,
            "taxi_type": [taxi]*10,
            "payment_type_id": [1]*10
        })

        data.append(df)

    final_df = pd.concat(data, ignore_index=True)
    final_df["extracted_at"] = datetime.utcnow()

    return final_df
