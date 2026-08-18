## Azure Databricks Processing Layer

Azure Databricks was implemented as the processing layer for the **Az-IOT-Berkeley — IoT Berkeley Lab Analytics Platform** project.

The main responsibility of this phase was to transform raw IoT files stored in ADLS Gen2 into structured, validated and analytics-ready Delta tables using PySpark, Delta Lake and Unity Catalog.

This phase covers the complete lakehouse processing flow:

```text
Landing → Bronze → Silver → Gold
```

Azure Data Factory was responsible only for file ingestion into the landing zone. Databricks was responsible for parsing, cleansing, validation, enrichment and analytical modeling.

### Databricks Architecture

The Databricks implementation uses Unity Catalog to organize the lakehouse objects into a clear logical structure.

Catalog:

```text
iot_berkeley_lab
```

Schemas:

```text
bronze
silver
gold
```

The physical data is stored in ADLS Gen2 under the following base path:

```text
abfss://iot-berkeley-lab@saiotberkeley.dfs.core.windows.net/
```

The main storage layers are:

```text
landing/
bronze/
silver/
gold/
rejected/
```

Unity Catalog was configured through a Storage Credential and External Location to provide governed access to ADLS Gen2 without using traditional mounts.

### Notebook Flow

The Databricks processing phase was implemented using six notebooks.

#### 00_setup_config

This notebook centralizes the project configuration used by the rest of the processing notebooks.

It defines:

* ADLS base paths.
* Landing, Bronze, Silver, Gold and Rejected paths.
* Unity Catalog table names.
* Catalog and schema names.
* Ingestion date parameter.
* Helper functions to detect the latest available ingestion folder and validate source files.

The ingestion date can be provided manually or detected automatically from the latest `ingest_date=YYYY-MM-DD` folder available in the landing layer.

#### 01_landing_to_bronze

This notebook reads the raw source files from the landing zone and writes them as Bronze Delta tables.

Inputs:

```text
landing/sensor_readings/ingest_date=YYYY-MM-DD/data.txt.gz
landing/sensor_metadata/ingest_date=YYYY-MM-DD/mote_locs.txt
```

Outputs:

```text
iot_berkeley_lab.bronze.sensor_readings_raw
iot_berkeley_lab.bronze.sensor_metadata_raw
```

The source files do not contain headers, so they are read as raw text. Each source record is preserved as a single `raw_line`.

The Bronze layer includes only technical metadata with a clear purpose:

* `raw_line`: preserves the original source record.
* `ingest_date`: identifies the processed ingestion batch.
* `source_file_path`: keeps lineage to the original ADLS file.
* `ingestion_timestamp`: records when Databricks processed the file.
* `record_hash`: provides a stable fingerprint for duplicate detection and reprocessing.

#### 02_bronze_to_silver

This notebook transforms raw Bronze records into clean and validated Silver Delta tables.

Inputs:

```text
iot_berkeley_lab.bronze.sensor_readings_raw
iot_berkeley_lab.bronze.sensor_metadata_raw
```

Outputs:

```text
iot_berkeley_lab.silver.sensor_readings_clean
iot_berkeley_lab.silver.sensor_metadata_clean
iot_berkeley_lab.silver.sensor_readings_enriched
```

The raw sensor readings are manually parsed using whitespace splitting because the source file does not include column headers.

Expected sensor reading fields:

```text
date
time
epoch
moteid
temperature
humidity
light
voltage
```

Expected metadata fields:

```text
mote_id
x_position
y_position
```

The Silver layer performs:

* Manual parsing.
* Type conversion.
* Event timestamp creation.
* Event date creation.
* Null validation.
* Basic numeric range validation.
* Deduplication using `reading_id`.
* Enrichment with sensor metadata.
* Separation of invalid records into the rejected zone.

Invalid records are written to:

```text
rejected/invalid_schema/
rejected/null_sensor_id/
rejected/invalid_ranges/
```

The Silver layer keeps technical lineage columns where they still provide value, such as `source_file_path` and `bronze_record_hash`.

#### 03_silver_to_gold_dimensions

This notebook creates the Gold dimension tables.

Inputs:

```text
iot_berkeley_lab.silver.sensor_readings_clean
iot_berkeley_lab.silver.sensor_metadata_clean
iot_berkeley_lab.silver.sensor_readings_enriched
```

Outputs:

```text
iot_berkeley_lab.gold.dim_mote
iot_berkeley_lab.gold.dim_date
iot_berkeley_lab.gold.dim_time
```

The Gold dimensions are:

##### dim_mote

Grain: one row per sensor.

Columns:

* `mote_id`
* `x_position`
* `y_position`
* `metadata_available`

##### dim_date

Grain: one row per event date.

Columns:

* `date_key`
* `full_date`
* `calendar_year`
* `calendar_quarter`
* `month_number`
* `month_name`
* `day_of_month`
* `day_of_week_number`
* `day_name`
* `is_weekend`

##### dim_time

Grain: one row per event time at minute level.

Columns:

* `time_key`
* `event_hour`
* `event_minute`
* `time_label`
* `day_period`

Dimension keys were validated to ensure they were not null and were unique.

#### 04_silver_to_gold_facts

This notebook creates the Gold fact tables.

Inputs:

```text
iot_berkeley_lab.silver.sensor_readings_clean
iot_berkeley_lab.silver.sensor_readings_enriched
iot_berkeley_lab.gold.dim_date
iot_berkeley_lab.gold.dim_time
iot_berkeley_lab.gold.dim_mote
```

Outputs:

```text
iot_berkeley_lab.gold.fact_sensor_reading
iot_berkeley_lab.gold.fact_sensor_hourly
iot_berkeley_lab.gold.fact_sensor_daily
iot_berkeley_lab.gold.fact_sensor_health
```

The Gold facts are:

##### fact_sensor_reading

Grain: one row per valid sensor reading.

Key columns:

* `reading_id`
* `date_key`
* `time_key`
* `mote_id`

Main measures:

* `temperature_celsius`
* `humidity_percent`
* `light`
* `voltage`

##### fact_sensor_hourly

Grain: one row per date and hour.

Main measures:

* `reading_count`
* `active_sensor_count`
* `avg_temperature_celsius`
* `min_temperature_celsius`
* `max_temperature_celsius`
* `avg_humidity_percent`
* `avg_light`
* `avg_voltage`
* `min_voltage`
* `low_voltage_reading_count`

##### fact_sensor_daily

Grain: one row per date.

Main measures:

* `reading_count`
* `active_sensor_count`
* `avg_temperature_celsius`
* `min_temperature_celsius`
* `max_temperature_celsius`
* `avg_humidity_percent`
* `avg_light`
* `avg_voltage`
* `min_voltage`
* `low_voltage_reading_count`

##### fact_sensor_health

Grain: one row per sensor and date.

Main measures:

* `reading_count`
* `first_reading_timestamp`
* `last_reading_timestamp`
* `avg_voltage`
* `min_voltage`
* `low_voltage_reading_count`
* `low_voltage_detected`

A low voltage threshold of `2.4` was used to identify possible low battery behavior.

Gold tables intentionally exclude Bronze/Silver technical columns such as `raw_line`, `source_file_path` and `bronze_record_hash`, because those columns do not provide direct analytical value in the final model.

#### 05_quality_checks

This notebook validates the final Databricks outputs without creating persistent log tables.

It checks:

* Null values in critical Silver columns.
* Invalid numeric ranges.
* Duplicate `reading_id` values.
* Missing dimension keys.
* Sensors without metadata.
* Low voltage behavior.
* Record count consistency between Silver and Gold.
* Rejected record counts.

The notebook fails only if critical checks fail. Warnings and informational checks are displayed for review but are not stored as permanent logs.

### Delta Lake Writing Strategy

All Bronze, Silver and Gold outputs were written as Delta tables.

The project uses explicit ADLS paths together with Unity Catalog table registration. This keeps the physical lake structure easy to understand while still allowing governed table access through Unity Catalog.

Example pattern:

```python
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("path", target_path) \
  .saveAsTable(target_table)
```

For Bronze and Silver tables, `ingest_date` is used to support partitioned reprocessing. The notebooks use `replaceWhere` when applicable to avoid duplicating records if the same ingestion date is processed again.

### Final Gold Data Model

The final analytical model contains three dimensions and four facts.

Dimensions:

```text
gold.dim_mote
gold.dim_date
gold.dim_time
```

Facts:

```text
gold.fact_sensor_reading
gold.fact_sensor_hourly
gold.fact_sensor_daily
gold.fact_sensor_health
```

Expected relationships:

```text
dim_date.date_key 1 → * fact_sensor_reading.date_key
dim_time.time_key 1 → * fact_sensor_reading.time_key
dim_mote.mote_id 1 → * fact_sensor_reading.mote_id

dim_date.date_key 1 → * fact_sensor_hourly.date_key

dim_date.date_key 1 → * fact_sensor_daily.date_key

dim_date.date_key 1 → * fact_sensor_health.date_key
dim_mote.mote_id 1 → * fact_sensor_health.mote_id
```
