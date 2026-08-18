# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05_quality_checks
# MAGIC
# MAGIC This notebook validates the quality of the IoT Berkeley Lab Analytics Platform after processing data from Bronze to Silver and Gold.
# MAGIC
# MAGIC Objective:
# MAGIC - Validate that Silver clean tables do not contain critical data quality issues.
# MAGIC - Validate that Gold fact and dimension tables have usable relationship keys.
# MAGIC - Validate duplicate records.
# MAGIC - Validate invalid numeric ranges.
# MAGIC - Validate low voltage behavior as a sensor health signal.
# MAGIC - Validate sensors without metadata.
# MAGIC - Display a quality summary without creating persistent log tables.
# MAGIC
# MAGIC Inputs:
# MAGIC - iot_berkeley_lab.bronze.sensor_readings_raw
# MAGIC - iot_berkeley_lab.bronze.sensor_metadata_raw
# MAGIC - iot_berkeley_lab.silver.sensor_readings_clean
# MAGIC - iot_berkeley_lab.silver.sensor_metadata_clean
# MAGIC - iot_berkeley_lab.silver.sensor_readings_enriched
# MAGIC - iot_berkeley_lab.gold.dim_mote
# MAGIC - iot_berkeley_lab.gold.dim_date
# MAGIC - iot_berkeley_lab.gold.dim_time
# MAGIC - iot_berkeley_lab.gold.fact_sensor_reading
# MAGIC - iot_berkeley_lab.gold.fact_sensor_hourly
# MAGIC - iot_berkeley_lab.gold.fact_sensor_daily
# MAGIC - iot_berkeley_lab.gold.fact_sensor_health
# MAGIC
# MAGIC Outputs:
# MAGIC - Temporary quality summary displayed in the notebook.
# MAGIC - Notebook failure if critical quality checks fail.
# MAGIC
# MAGIC Processing flow:
# MAGIC 1. Load shared configuration.
# MAGIC 2. Read Bronze, Silver and Gold tables.
# MAGIC 3. Validate critical null values.
# MAGIC 4. Validate numeric ranges.
# MAGIC 5. Validate duplicate records.
# MAGIC 6. Validate Gold relationships.
# MAGIC 7. Validate sensors without metadata.
# MAGIC 8. Review low voltage behavior.
# MAGIC 9. Display final quality summary.
# MAGIC 10. Fail the notebook if critical checks fail.
# MAGIC
# MAGIC Important:
# MAGIC This notebook does not create persistent log tables.
# MAGIC Only checks with clear analytical or data engineering value are included.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load shared project configuration
# MAGIC
# MAGIC This step imports the global configuration from 00_setup_config.
# MAGIC
# MAGIC The configuration provides ADLS paths, Unity Catalog table names and the selected ingestion date.

# COMMAND ----------

# DBTITLE 1,Load shared project configuration
# MAGIC %run ./00_setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import required PySpark functions
# MAGIC
# MAGIC This step imports the functions required for counting, filtering, validating duplicates and creating the quality summary.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import (
    col,
    lit,
    count,
    countDistinct,
    when,
    min as spark_min,
    max as spark_max,
    avg,
    sum as spark_sum
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read Bronze, Silver and Gold tables
# MAGIC
# MAGIC This step reads the tables created by the previous notebooks.
# MAGIC
# MAGIC Bronze and Silver tables are filtered by ingest_date.
# MAGIC Gold tables are already rebuilt from the selected processed batch.

# COMMAND ----------

# DBTITLE 1,Read Bronze, Silver and Gold tables
# Bronze
df_bronze_sensor_readings = (
    spark.table(bronze_sensor_readings_table)
    .filter(col("ingest_date") == ingest_date)
)

df_bronze_sensor_metadata = (
    spark.table(bronze_sensor_metadata_table)
    .filter(col("ingest_date") == ingest_date)
)

# Silver
df_silver_sensor_readings_clean = (
    spark.table(silver_sensor_readings_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_silver_sensor_metadata_clean = (
    spark.table(silver_sensor_metadata_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_silver_sensor_readings_enriched = (
    spark.table(silver_sensor_readings_enriched_table)
    .filter(col("ingest_date") == ingest_date)
)

# Gold dimensions
df_dim_mote = spark.table(gold_dim_mote_table)
df_dim_date = spark.table(gold_dim_date_table)
df_dim_time = spark.table(gold_dim_time_table)

# Gold facts
df_fact_sensor_reading = spark.table(gold_fact_sensor_reading_table)
df_fact_sensor_hourly = spark.table(gold_fact_sensor_hourly_table)
df_fact_sensor_daily = spark.table(gold_fact_sensor_daily_table)
df_fact_sensor_health = spark.table(gold_fact_sensor_health_table)

print(f"Bronze sensor readings count: {df_bronze_sensor_readings.count()}")
print(f"Bronze sensor metadata count: {df_bronze_sensor_metadata.count()}")

print(f"Silver sensor readings clean count: {df_silver_sensor_readings_clean.count()}")
print(f"Silver sensor metadata clean count: {df_silver_sensor_metadata_clean.count()}")
print(f"Silver sensor readings enriched count: {df_silver_sensor_readings_enriched.count()}")

print(f"Gold fact sensor reading count: {df_fact_sensor_reading.count()}")
print(f"Gold fact sensor hourly count: {df_fact_sensor_hourly.count()}")
print(f"Gold fact sensor daily count: {df_fact_sensor_daily.count()}")
print(f"Gold fact sensor health count: {df_fact_sensor_health.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Define quality check helpers
# MAGIC
# MAGIC This step defines helper functions to standardize quality results.
# MAGIC
# MAGIC The quality checks are stored only in memory and displayed at the end of the notebook.
# MAGIC They are not written as persistent logs.

# COMMAND ----------

# DBTITLE 1,Define quality check helpers
quality_results = []

def add_quality_result(check_name: str, severity: str, metric_value: int, expected_value: str, status: str):
    """
    Adds one quality check result to the in-memory quality summary.

    severity:
    - critical: fails the notebook if status is FAIL.
    - warning: does not fail the notebook, but should be reviewed.
    - info: informational metric.
    """

    quality_results.append(
        (
            check_name,
            severity,
            int(metric_value),
            expected_value,
            status
        )
    )


def evaluate_zero_count_check(check_name: str, severity: str, metric_value: int):
    """
    Evaluates checks where the expected result is zero.
    """

    status = "PASS" if metric_value == 0 else "FAIL"

    add_quality_result(
        check_name=check_name,
        severity=severity,
        metric_value=metric_value,
        expected_value="0",
        status=status
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Validate critical null values in Silver
# MAGIC
# MAGIC This step validates that Silver clean readings do not contain null values in fields required for analysis.
# MAGIC
# MAGIC These fields are critical because they are used for facts, dimensions, aggregations and relationships.

# COMMAND ----------

# DBTITLE 1,Validate critical null values in Silver
critical_silver_columns = [
    "reading_id",
    "event_timestamp",
    "event_date",
    "epoch",
    "mote_id",
    "temperature_celsius",
    "humidity_percent",
    "light",
    "voltage"
]

for column_name in critical_silver_columns:
    null_count = df_silver_sensor_readings_clean.filter(col(column_name).isNull()).count()

    evaluate_zero_count_check(
        check_name=f"silver_sensor_readings_clean_null_{column_name}",
        severity="critical",
        metric_value=null_count
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Validate numeric ranges in Silver
# MAGIC
# MAGIC This step validates that Silver clean readings do not contain values outside the accepted basic ranges.
# MAGIC
# MAGIC Expected ranges:
# MAGIC - temperature_celsius between -40 and 85.
# MAGIC - humidity_percent between 0 and 100.
# MAGIC - light greater than or equal to 0.
# MAGIC - voltage greater than 0 and less than or equal to 5.

# COMMAND ----------

# DBTITLE 1,Validate numeric ranges in Silver
invalid_temperature_count = (
    df_silver_sensor_readings_clean
    .filter((col("temperature_celsius") < -40) | (col("temperature_celsius") > 85))
    .count()
)

invalid_humidity_count = (
    df_silver_sensor_readings_clean
    .filter((col("humidity_percent") < 0) | (col("humidity_percent") > 100))
    .count()
)

invalid_light_count = (
    df_silver_sensor_readings_clean
    .filter(col("light") < 0)
    .count()
)

invalid_voltage_count = (
    df_silver_sensor_readings_clean
    .filter((col("voltage") <= 0) | (col("voltage") > 5))
    .count()
)

evaluate_zero_count_check("silver_invalid_temperature_range", "critical", invalid_temperature_count)
evaluate_zero_count_check("silver_invalid_humidity_range", "critical", invalid_humidity_count)
evaluate_zero_count_check("silver_invalid_light_range", "critical", invalid_light_count)
evaluate_zero_count_check("silver_invalid_voltage_range", "critical", invalid_voltage_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Validate duplicate readings
# MAGIC
# MAGIC This step validates that reading_id is unique in Silver and Gold.
# MAGIC
# MAGIC A duplicated reading_id means the same reading was processed more than once into analytical tables.

# COMMAND ----------

# DBTITLE 1,Validate duplicate readings
silver_duplicate_reading_id_count = (
    df_silver_sensor_readings_clean
    .groupBy("reading_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)

gold_duplicate_reading_id_count = (
    df_fact_sensor_reading
    .groupBy("reading_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)

evaluate_zero_count_check(
    check_name="silver_duplicate_reading_id",
    severity="critical",
    metric_value=silver_duplicate_reading_id_count
)

evaluate_zero_count_check(
    check_name="gold_duplicate_reading_id",
    severity="critical",
    metric_value=gold_duplicate_reading_id_count
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate Gold relationship keys
# MAGIC
# MAGIC This step validates that fact table keys can connect to Gold dimensions.
# MAGIC
# MAGIC The fact table must not contain date_key, time_key or mote_id values that are missing from the dimensions.

# COMMAND ----------

# DBTITLE 1,Validate Gold relationship keys
missing_date_keys_count = (
    df_fact_sensor_reading
    .select("date_key")
    .distinct()
    .join(df_dim_date.select("date_key").distinct(), on="date_key", how="left_anti")
    .count()
)

missing_time_keys_count = (
    df_fact_sensor_reading
    .select("time_key")
    .distinct()
    .join(df_dim_time.select("time_key").distinct(), on="time_key", how="left_anti")
    .count()
)

missing_mote_ids_count = (
    df_fact_sensor_reading
    .select("mote_id")
    .distinct()
    .join(df_dim_mote.select("mote_id").distinct(), on="mote_id", how="left_anti")
    .count()
)

evaluate_zero_count_check("gold_fact_missing_date_keys", "critical", missing_date_keys_count)
evaluate_zero_count_check("gold_fact_missing_time_keys", "critical", missing_time_keys_count)
evaluate_zero_count_check("gold_fact_missing_mote_ids", "critical", missing_mote_ids_count)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate sensors without metadata
# MAGIC
# MAGIC This step identifies valid sensor readings that do not have matching sensor metadata.
# MAGIC
# MAGIC This is treated as a warning, not a critical error, because the reading is still analytically valid, but location-based analysis may be incomplete.

# COMMAND ----------

# DBTITLE 1,Validate sensors without metadata
readings_without_metadata_count = (
    df_silver_sensor_readings_enriched
    .filter(col("x_position").isNull() | col("y_position").isNull())
    .count()
)

status = "PASS" if readings_without_metadata_count == 0 else "WARN"

add_quality_result(
    check_name="silver_readings_without_metadata",
    severity="warning",
    metric_value=readings_without_metadata_count,
    expected_value="0 preferred",
    status=status
)

display(
    df_silver_sensor_readings_enriched
    .filter(col("x_position").isNull() | col("y_position").isNull())
    .select("mote_id")
    .distinct()
    .orderBy("mote_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Review low voltage behavior
# MAGIC
# MAGIC This step summarizes low voltage readings.
# MAGIC
# MAGIC Low voltage is not a data quality failure. It is a sensor health signal that will be analyzed through fact_sensor_health.

# COMMAND ----------

# DBTITLE 1,Review low voltage behavior
low_voltage_threshold = 2.4

low_voltage_reading_count = (
    df_fact_sensor_reading
    .filter(col("voltage") < low_voltage_threshold)
    .count()
)

sensors_with_low_voltage_count = (
    df_fact_sensor_reading
    .filter(col("voltage") < low_voltage_threshold)
    .select("mote_id")
    .distinct()
    .count()
)

add_quality_result(
    check_name="gold_low_voltage_reading_count",
    severity="info",
    metric_value=low_voltage_reading_count,
    expected_value=f"informational, threshold < {low_voltage_threshold}",
    status="INFO"
)

add_quality_result(
    check_name="gold_sensors_with_low_voltage",
    severity="info",
    metric_value=sensors_with_low_voltage_count,
    expected_value=f"informational, threshold < {low_voltage_threshold}",
    status="INFO"
)

display(
    df_fact_sensor_health
    .filter(col("low_voltage_detected") == True)
    .orderBy("date_key", "mote_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Validate table count consistency
# MAGIC
# MAGIC This step compares record counts across Silver and Gold.
# MAGIC
# MAGIC The detailed Gold fact should have the same number of records as the Silver clean readings table.
# MAGIC The enriched Silver table should also keep the same number of readings after the left join with metadata.

# COMMAND ----------

# DBTITLE 1,Validate table count consistency
silver_clean_count = df_silver_sensor_readings_clean.count()
silver_enriched_count = df_silver_sensor_readings_enriched.count()
gold_fact_reading_count = df_fact_sensor_reading.count()

silver_to_enriched_difference = silver_clean_count - silver_enriched_count
silver_to_gold_difference = silver_clean_count - gold_fact_reading_count

evaluate_zero_count_check(
    check_name="silver_clean_vs_silver_enriched_count_difference",
    severity="critical",
    metric_value=silver_to_enriched_difference
)

evaluate_zero_count_check(
    check_name="silver_clean_vs_gold_fact_reading_count_difference",
    severity="critical",
    metric_value=silver_to_gold_difference
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Review rejected records from Silver processing
# MAGIC
# MAGIC This step reads rejected Delta paths if they exist.
# MAGIC
# MAGIC Rejected records are not logs. They are invalid records separated from the analytical layer because they do not meet Silver quality rules.

# COMMAND ----------

# DBTITLE 1,Review rejected records from Silver processing
def delta_path_count_if_exists(path: str) -> int:
    """
    Returns the number of records in a Delta path if it exists.
    Returns 0 if the path does not exist or has no Delta table.
    """

    try:
        return spark.read.format("delta").load(path).filter(col("ingest_date") == ingest_date).count()
    except Exception:
        return 0


rejected_sensor_readings_invalid_schema_count = delta_path_count_if_exists(
    f"{rejected_invalid_schema_path}sensor_readings/"
)

rejected_sensor_readings_null_sensor_id_count = delta_path_count_if_exists(
    f"{rejected_null_sensor_id_path}sensor_readings/"
)

rejected_sensor_readings_invalid_ranges_count = delta_path_count_if_exists(
    f"{rejected_invalid_ranges_path}sensor_readings/"
)

rejected_sensor_metadata_invalid_schema_count = delta_path_count_if_exists(
    f"{rejected_invalid_schema_path}sensor_metadata/"
)

rejected_sensor_metadata_null_sensor_id_count = delta_path_count_if_exists(
    f"{rejected_null_sensor_id_path}sensor_metadata/"
)

rejected_sensor_metadata_invalid_values_count = delta_path_count_if_exists(
    f"{rejected_invalid_ranges_path}sensor_metadata/"
)

add_quality_result(
    "rejected_sensor_readings_invalid_schema",
    "info",
    rejected_sensor_readings_invalid_schema_count,
    "informational",
    "INFO"
)

add_quality_result(
    "rejected_sensor_readings_null_sensor_id",
    "info",
    rejected_sensor_readings_null_sensor_id_count,
    "informational",
    "INFO"
)

add_quality_result(
    "rejected_sensor_readings_invalid_ranges",
    "info",
    rejected_sensor_readings_invalid_ranges_count,
    "informational",
    "INFO"
)

add_quality_result(
    "rejected_sensor_metadata_invalid_schema",
    "info",
    rejected_sensor_metadata_invalid_schema_count,
    "informational",
    "INFO"
)

add_quality_result(
    "rejected_sensor_metadata_null_sensor_id",
    "info",
    rejected_sensor_metadata_null_sensor_id_count,
    "informational",
    "INFO"
)

add_quality_result(
    "rejected_sensor_metadata_invalid_values",
    "info",
    rejected_sensor_metadata_invalid_values_count,
    "informational",
    "INFO"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Display final quality summary
# MAGIC
# MAGIC This step displays the final quality summary.
# MAGIC
# MAGIC The summary is temporary and is not saved as a log table.

# COMMAND ----------

# DBTITLE 1,Display final quality summary
df_quality_summary = spark.createDataFrame(
    quality_results,
    [
        "check_name",
        "severity",
        "metric_value",
        "expected_value",
        "status"
    ]
)

display(df_quality_summary.orderBy("severity", "check_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Fail notebook if critical checks failed
# MAGIC
# MAGIC This step fails the notebook only when critical checks fail.
# MAGIC
# MAGIC Warnings and informational checks are displayed for review but do not stop the pipeline.

# COMMAND ----------

# DBTITLE 1,Fail notebook if critical checks failed
critical_failure_count = (
    df_quality_summary
    .filter((col("severity") == "critical") & (col("status") == "FAIL"))
    .count()
)

warning_count = (
    df_quality_summary
    .filter(col("status") == "WARN")
    .count()
)

print(f"Critical failure count: {critical_failure_count}")
print(f"Warning count: {warning_count}")

if critical_failure_count > 0:
    display(
        df_quality_summary
        .filter((col("severity") == "critical") & (col("status") == "FAIL"))
    )

    raise ValueError("Critical data quality checks failed. Review the quality summary.")

print("All critical data quality checks passed.")