/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: dropoff_datetime
    type: timestamp

  - name: trip_date
    type: date
    nullable: false
    checks:
      - name: not_null

  - name: pickup_location_id
    type: integer

  - name: dropoff_location_id
    type: integer

  - name: fare_amount
    type: double
    checks:
      - name: non_negative

  - name: taxi_type
    type: string

  - name: payment_type_name
    type: string

custom_checks:
  - name: no_null_fares
    description: Ensure fare_amount is never NULL
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE fare_amount IS NULL
    value: 0

@bruin */

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    DATE(t.pickup_datetime) AS trip_date,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    pl.payment_type_name

FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup pl
  ON t.payment_type_id = pl.payment_type_id

WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
  AND t.fare_amount >= 0
