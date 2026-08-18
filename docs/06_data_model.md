# Data Model

## Overview

The final analytical model is created in the Databricks Gold layer and exposed through Synapse Serverless SQL views.

The model follows a simple dimensional approach with three dimensions and four fact tables.

## Gold Dimensions

### `gold.dim_mote`

Grain: one row per sensor.

Columns:

- `mote_id`
- `x_position`
- `y_position`
- `metadata_available`

Purpose: provides sensor metadata and indicates whether each mote has available location metadata.

### `gold.dim_date`

Grain: one row per event date.

Columns:

- `date_key`
- `full_date`
- `calendar_year`
- `calendar_quarter`
- `month_number`
- `month_name`
- `day_of_month`
- `day_of_week_number`
- `day_name`
- `is_weekend`

Purpose: supports calendar-based analysis.

### `gold.dim_time`

Grain: one row per event time at minute level.

Columns:

- `time_key`
- `event_hour`
- `event_minute`
- `time_label`
- `day_period`

Purpose: supports hourly, minute-level, and day-period analysis.

## Gold Facts

### `gold.fact_sensor_reading`

Grain: one row per valid sensor reading.

Key columns:

- `reading_id`
- `date_key`
- `time_key`
- `mote_id`

Measures:

- `temperature_celsius`
- `humidity_percent`
- `light`
- `voltage`

### `gold.fact_sensor_hourly`

Grain: one row per date and hour.

Measures:

- `reading_count`
- `active_sensor_count`
- `avg_temperature_celsius`
- `min_temperature_celsius`
- `max_temperature_celsius`
- `avg_humidity_percent`
- `avg_light`
- `avg_voltage`
- `min_voltage`
- `low_voltage_reading_count`

### `gold.fact_sensor_daily`

Grain: one row per date.

Measures:

- `reading_count`
- `active_sensor_count`
- `avg_temperature_celsius`
- `min_temperature_celsius`
- `max_temperature_celsius`
- `avg_humidity_percent`
- `avg_light`
- `avg_voltage`
- `min_voltage`
- `low_voltage_reading_count`

### `gold.fact_sensor_health`

Grain: one row per sensor and date.

Measures:

- `reading_count`
- `first_reading_timestamp`
- `last_reading_timestamp`
- `avg_voltage`
- `min_voltage`
- `low_voltage_reading_count`
- `low_voltage_detected`

A low voltage threshold of `2.4` was used to identify possible low battery behavior.

## Expected Relationships

```text
dim_date.date_key 1 → * fact_sensor_reading.date_key
dim_time.time_key 1 → * fact_sensor_reading.time_key
dim_mote.mote_id 1 → * fact_sensor_reading.mote_id

dim_date.date_key 1 → * fact_sensor_hourly.date_key

dim_date.date_key 1 → * fact_sensor_daily.date_key

dim_date.date_key 1 → * fact_sensor_health.date_key
dim_mote.mote_id 1 → * fact_sensor_health.mote_id
```

## Serving Views

Synapse Serverless SQL exposes the Gold model through these views:

| Gold Table | Synapse View |
| --- | --- |
| `gold.dim_mote` | `dbo.vw_dim_mote` |
| `gold.dim_date` | `dbo.vw_dim_date` |
| `gold.dim_time` | `dbo.vw_dim_time` |
| `gold.fact_sensor_reading` | `dbo.vw_fact_sensor_reading` |
| `gold.fact_sensor_hourly` | `dbo.vw_fact_sensor_hourly` |
| `gold.fact_sensor_daily` | `dbo.vw_fact_sensor_daily` |
| `gold.fact_sensor_health` | `dbo.vw_fact_sensor_health` |
