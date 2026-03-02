/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: taxi_type
    type: string
    description: Taxi type (yellow/green)
    primary_key: true

  - name: trip_date
    type: date
    description: Trip date
    primary_key: true

  - name: total_trips
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative

  - name: total_revenue
    type: double
    description: Total revenue per day per taxi type
    checks:
      - name: non_negative

@bruin */

SELECT
    taxi_type,
    trip_date,
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_revenue

FROM staging.trips

WHERE trip_date >= DATE('{{ start_datetime }}')
  AND trip_date < DATE('{{ end_datetime }}')

GROUP BY
    taxi_type,
    trip_date
