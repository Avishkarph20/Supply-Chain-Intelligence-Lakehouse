# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02_bronze_to_silver
# MAGIC
# MAGIC This notebook transforms raw bronze IoT Berkeley Lab data into clean and enriched silver Delta tables.
# MAGIC
# MAGIC Objective:
# MAGIC - Read raw sensor readings from the bronze Delta table.
# MAGIC - Read raw sensor metadata from the bronze Delta table.
# MAGIC - Parse raw text lines manually because the source files do not contain headers.
# MAGIC - Convert source values into proper data types.
# MAGIC - Validate schema, sensor identifiers, timestamps and basic numeric ranges.
# MAGIC - Write invalid records to the rejected zone when they have no analytical value in silver.
# MAGIC - Create clean silver tables for sensor readings and sensor metadata.
# MAGIC - Create an enriched silver table by joining sensor readings with sensor metadata.
# MAGIC
# MAGIC Inputs:
# MAGIC - iot_berkeley_lab.bronze.sensor_readings_raw
# MAGIC - iot_berkeley_lab.bronze.sensor_metadata_raw
# MAGIC
# MAGIC Outputs:
# MAGIC - iot_berkeley_lab.silver.sensor_readings_clean
# MAGIC - iot_berkeley_lab.silver.sensor_metadata_clean
# MAGIC - iot_berkeley_lab.silver.sensor_readings_enriched
# MAGIC - rejected/invalid_schema/
# MAGIC - rejected/null_sensor_id/
# MAGIC - rejected/invalid_ranges/
# MAGIC
# MAGIC Processing flow:
# MAGIC 1. Load global configuration.
# MAGIC 2. Read bronze tables for the selected ingestion date.
# MAGIC 3. Parse raw sensor readings.
# MAGIC 4. Separate invalid sensor reading records.
# MAGIC 5. Create clean sensor readings.
# MAGIC 6. Parse and clean sensor metadata.
# MAGIC 7. Create enriched sensor readings.
# MAGIC 8. Write silver Delta tables.
# MAGIC 9. Write rejected records only when invalid records exist.
# MAGIC 10. Validate final results.
# MAGIC
# MAGIC Important:
# MAGIC The source files do not contain headers.
# MAGIC This notebook manually assigns the expected column meaning after splitting each raw line.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load shared project configuration
# MAGIC
# MAGIC This step imports the global project configuration from 00_setup_config.
# MAGIC
# MAGIC The configuration provides:
# MAGIC - ADLS paths.
# MAGIC - Unity Catalog table names.
# MAGIC - Selected ingestion date.
# MAGIC - Source and target locations.

# COMMAND ----------

# DBTITLE 1,Load shared project configuration
# MAGIC %run ./00_setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import required PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions required for parsing, type conversion, validation, hashing, joins and aggregations.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import (
    col,
    trim,
    split,
    size,
    element_at,
    concat_ws,
    sha2,
    to_timestamp,
    to_date,
    lit,
    when,
    count
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read bronze tables for the selected ingestion date
# MAGIC
# MAGIC This step reads only the bronze records that belong to the selected ingest_date.
# MAGIC
# MAGIC Filtering by ingest_date keeps the notebook focused on the current ingestion batch and avoids reprocessing all historical data unnecessarily.

# COMMAND ----------

# DBTITLE 1,Read bronze tables for the selected ingestion date
df_bronze_sensor_readings = (
    spark.table(bronze_sensor_readings_table)
    .filter(col("ingest_date") == ingest_date)
)

df_bronze_sensor_metadata = (
    spark.table(bronze_sensor_metadata_table)
    .filter(col("ingest_date") == ingest_date)
)

print(f"Bronze sensor readings count: {df_bronze_sensor_readings.count()}")
print(f"Bronze sensor metadata count: {df_bronze_sensor_metadata.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Split raw sensor reading lines into tokens
# MAGIC
# MAGIC This step splits each raw sensor reading line by whitespace.
# MAGIC
# MAGIC Expected source order:
# MAGIC 1. date
# MAGIC 2. time
# MAGIC 3. epoch
# MAGIC 4. moteid
# MAGIC 5. temperature
# MAGIC 6. humidity
# MAGIC 7. light
# MAGIC 8. voltage
# MAGIC
# MAGIC The source file has no headers, so the meaning of each value is assigned manually.

# COMMAND ----------

# DBTITLE 1,Split raw sensor reading lines into tokens
df_sensor_readings_tokens = (
    df_bronze_sensor_readings
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("record_hash").alias("bronze_record_hash")
    )
    .withColumn("tokens", split(trim(col("raw_line")), r"\s+"))
    .withColumn("token_count", size(col("tokens")))
)

display(df_sensor_readings_tokens.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Separate sensor reading records with invalid schema
# MAGIC
# MAGIC This step identifies records that do not have exactly 8 values.
# MAGIC
# MAGIC Records with an invalid number of values cannot be safely parsed into the expected sensor reading structure, so they are separated into the rejected zone.

# COMMAND ----------

# DBTITLE 1,Separate sensor reading records with invalid schema
df_sensor_readings_invalid_schema = (
    df_sensor_readings_tokens
    .filter(col("token_count") != 8)
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash"),
        lit("invalid_schema").alias("rejection_reason")
    )
)

df_sensor_readings_valid_schema = (
    df_sensor_readings_tokens
    .filter(col("token_count") == 8)
)

display(df_sensor_readings_invalid_schema.limit(10))

print(f"Sensor readings invalid schema count: {df_sensor_readings_invalid_schema.count()}")
print(f"Sensor readings valid schema count: {df_sensor_readings_valid_schema.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Parse sensor reading columns and convert data types
# MAGIC
# MAGIC This step assigns business meaning to each token and converts values into proper data types.
# MAGIC
# MAGIC Temporary string columns are used only during parsing and are not kept in the final silver table because they would duplicate event_timestamp and event_date.

# COMMAND ----------

# DBTITLE 1,Parse sensor reading columns and convert data types
df_sensor_readings_parsed = (
    df_sensor_readings_valid_schema
    .withColumn("source_date_string", element_at(col("tokens"), 1))
    .withColumn("source_time_string", element_at(col("tokens"), 2))
    .withColumn("event_timestamp", to_timestamp(concat_ws(" ", col("source_date_string"), col("source_time_string"))))
    .withColumn("event_date", to_date(col("event_timestamp")))
    .withColumn("epoch", element_at(col("tokens"), 3).cast("long"))
    .withColumn("mote_id", element_at(col("tokens"), 4).cast("int"))
    .withColumn("temperature_celsius", element_at(col("tokens"), 5).cast("double"))
    .withColumn("humidity_percent", element_at(col("tokens"), 6).cast("double"))
    .withColumn("light", element_at(col("tokens"), 7).cast("double"))
    .withColumn("voltage", element_at(col("tokens"), 8).cast("double"))
)

display(df_sensor_readings_parsed.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Separate records with null sensor identifier
# MAGIC
# MAGIC This step separates records where mote_id could not be parsed.
# MAGIC
# MAGIC A sensor reading without a sensor identifier cannot be reliably joined to metadata or analyzed by sensor, so it is rejected.

# COMMAND ----------

# DBTITLE 1,Separate records with null sensor identifier
df_sensor_readings_null_sensor_id = (
    df_sensor_readings_parsed
    .filter(col("mote_id").isNull())
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash"),
        lit("null_sensor_id").alias("rejection_reason")
    )
)

df_sensor_readings_with_sensor_id = (
    df_sensor_readings_parsed
    .filter(col("mote_id").isNotNull())
)

print(f"Sensor readings null sensor id count: {df_sensor_readings_null_sensor_id.count()}")
print(f"Sensor readings with sensor id count: {df_sensor_readings_with_sensor_id.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate basic sensor reading ranges
# MAGIC
# MAGIC This step applies basic validation rules.
# MAGIC
# MAGIC Validation rules:
# MAGIC - event_timestamp must not be null.
# MAGIC - epoch must not be null.
# MAGIC - temperature_celsius must be between -40 and 85.
# MAGIC - humidity_percent must be between 0 and 100.
# MAGIC - light must be greater than or equal to 0.
# MAGIC - voltage must be greater than 0 and less than or equal to 5.
# MAGIC
# MAGIC These rules are intentionally basic. They remove clearly invalid records without overfitting the cleaning logic.

# COMMAND ----------

# DBTITLE 1,Validate basic sensor reading ranges
invalid_sensor_range_condition = (
    col("event_timestamp").isNull()
    | col("epoch").isNull()
    | col("temperature_celsius").isNull()
    | col("humidity_percent").isNull()
    | col("light").isNull()
    | col("voltage").isNull()
    | (col("temperature_celsius") < -40)
    | (col("temperature_celsius") > 85)
    | (col("humidity_percent") < 0)
    | (col("humidity_percent") > 100)
    | (col("light") < 0)
    | (col("voltage") <= 0)
    | (col("voltage") > 5)
)

df_sensor_readings_invalid_ranges = (
    df_sensor_readings_with_sensor_id
    .filter(invalid_sensor_range_condition)
    .withColumn(
        "rejection_reason",
        when(col("event_timestamp").isNull(), "invalid_event_timestamp")
        .when(col("epoch").isNull(), "invalid_epoch")
        .when(col("temperature_celsius").isNull(), "invalid_temperature")
        .when(col("humidity_percent").isNull(), "invalid_humidity")
        .when(col("light").isNull(), "invalid_light")
        .when(col("voltage").isNull(), "invalid_voltage")
        .when((col("temperature_celsius") < -40) | (col("temperature_celsius") > 85), "temperature_out_of_range")
        .when((col("humidity_percent") < 0) | (col("humidity_percent") > 100), "humidity_out_of_range")
        .when(col("light") < 0, "light_out_of_range")
        .when((col("voltage") <= 0) | (col("voltage") > 5), "voltage_out_of_range")
        .otherwise("invalid_range")
    )
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash"),
        col("rejection_reason")
    )
)

df_sensor_readings_clean = (
    df_sensor_readings_with_sensor_id
    .filter(~invalid_sensor_range_condition)
)

display(df_sensor_readings_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Create final clean sensor readings DataFrame
# MAGIC
# MAGIC This step creates the final silver sensor readings structure.
# MAGIC
# MAGIC Only columns with analytical, lineage or reprocessing value are kept.
# MAGIC Temporary parsing columns are removed.
# MAGIC Duplicate readings are removed using reading_id.

# COMMAND ----------

# DBTITLE 1,Create final clean sensor readings DataFrame
df_sensor_readings_clean_final = (
    df_sensor_readings_clean
    .withColumn(
        "reading_id",
        sha2(
            concat_ws(
                "||",
                col("event_timestamp").cast("string"),
                col("epoch").cast("string"),
                col("mote_id").cast("string"),
                col("temperature_celsius").cast("string"),
                col("humidity_percent").cast("string"),
                col("light").cast("string"),
                col("voltage").cast("string")
            ),
            256
        )
    )
    .select(
        col("reading_id"),
        col("event_timestamp"),
        col("event_date"),
        col("epoch"),
        col("mote_id"),
        col("temperature_celsius"),
        col("humidity_percent"),
        col("light"),
        col("voltage"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash")
    )
    .dropDuplicates(["reading_id"])
)

print(f"Sensor readings invalid range count: {df_sensor_readings_invalid_ranges.count()}")
print(f"Sensor readings clean final count: {df_sensor_readings_clean_final.count()}")

display(df_sensor_readings_clean_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Split raw sensor metadata lines into tokens
# MAGIC
# MAGIC This step splits the raw sensor metadata file by whitespace.
# MAGIC
# MAGIC Expected source order:
# MAGIC 1. mote_id
# MAGIC 2. x_position
# MAGIC 3. y_position
# MAGIC
# MAGIC The source file has no headers, so the column meaning is assigned manually.

# COMMAND ----------

# DBTITLE 1,Split raw sensor metadata lines into tokens
df_sensor_metadata_tokens = (
    df_bronze_sensor_metadata
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("record_hash").alias("bronze_record_hash")
    )
    .withColumn("tokens", split(trim(col("raw_line")), r"\s+"))
    .withColumn("token_count", size(col("tokens")))
)

display(df_sensor_metadata_tokens.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Parse and validate sensor metadata
# MAGIC
# MAGIC This step parses sensor metadata and separates invalid records.
# MAGIC
# MAGIC A valid metadata record must contain:
# MAGIC - exactly 3 values
# MAGIC - non-null mote_id
# MAGIC - non-null x_position
# MAGIC - non-null y_position

# COMMAND ----------

# DBTITLE 1,Parse and validate sensor metadata
df_sensor_metadata_invalid_schema = (
    df_sensor_metadata_tokens
    .filter(col("token_count") != 3)
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash"),
        lit("invalid_schema").alias("rejection_reason")
    )
)

df_sensor_metadata_valid_schema = (
    df_sensor_metadata_tokens
    .filter(col("token_count") == 3)
)

df_sensor_metadata_parsed = (
    df_sensor_metadata_valid_schema
    .withColumn("mote_id", element_at(col("tokens"), 1).cast("int"))
    .withColumn("x_position", element_at(col("tokens"), 2).cast("double"))
    .withColumn("y_position", element_at(col("tokens"), 3).cast("double"))
)

df_sensor_metadata_null_sensor_id = (
    df_sensor_metadata_parsed
    .filter(col("mote_id").isNull())
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash"),
        lit("null_sensor_id").alias("rejection_reason")
    )
)

df_sensor_metadata_invalid_values = (
    df_sensor_metadata_parsed
    .filter(
        col("mote_id").isNotNull()
        & (
            col("x_position").isNull()
            | col("y_position").isNull()
        )
    )
    .select(
        col("raw_line"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash"),
        lit("invalid_metadata_position").alias("rejection_reason")
    )
)

df_sensor_metadata_clean_final = (
    df_sensor_metadata_parsed
    .filter(
        col("mote_id").isNotNull()
        & col("x_position").isNotNull()
        & col("y_position").isNotNull()
    )
    .select(
        col("mote_id"),
        col("x_position"),
        col("y_position"),
        col("ingest_date"),
        col("source_file_path"),
        col("bronze_record_hash")
    )
    .dropDuplicates(["mote_id"])
)

print(f"Sensor metadata invalid schema count: {df_sensor_metadata_invalid_schema.count()}")
print(f"Sensor metadata null sensor id count: {df_sensor_metadata_null_sensor_id.count()}")
print(f"Sensor metadata invalid values count: {df_sensor_metadata_invalid_values.count()}")
print(f"Sensor metadata clean final count: {df_sensor_metadata_clean_final.count()}")

display(df_sensor_metadata_clean_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Create enriched sensor readings
# MAGIC
# MAGIC This step joins clean sensor readings with clean sensor metadata.
# MAGIC
# MAGIC The join uses mote_id.
# MAGIC
# MAGIC A left join is used because a valid reading should not be removed just because metadata is missing. Missing metadata will be reviewed later in quality checks.

# COMMAND ----------

# DBTITLE 1,Create enriched sensor readings
df_sensor_readings_enriched = (
    df_sensor_readings_clean_final.alias("r")
    .join(
        df_sensor_metadata_clean_final.alias("m"),
        on="mote_id",
        how="left"
    )
    .select(
        col("r.reading_id"),
        col("r.event_timestamp"),
        col("r.event_date"),
        col("r.epoch"),
        col("r.mote_id"),
        col("m.x_position"),
        col("m.y_position"),
        col("r.temperature_celsius"),
        col("r.humidity_percent"),
        col("r.light"),
        col("r.voltage"),
        col("r.ingest_date"),
        col("r.source_file_path"),
        col("r.bronze_record_hash")
    )
)

print(f"Sensor readings enriched count: {df_sensor_readings_enriched.count()}")

display(df_sensor_readings_enriched.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Define reusable Delta write functions
# MAGIC
# MAGIC This step defines helper functions to write silver tables and rejected records.
# MAGIC
# MAGIC Silver tables are registered in Unity Catalog.
# MAGIC Rejected records are written to ADLS rejected paths only when invalid records exist.

# COMMAND ----------

# DBTITLE 1,Define reusable Delta write functions
def write_delta_table_by_ingest_date(df, table_name: str, table_path: str):
    """
    Writes a DataFrame as a Delta table registered in Unity Catalog.

    The write replaces only the selected ingest_date partition.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingest_date = '{ingest_date}'")
        .option("path", table_path)
        .partitionBy("ingest_date")
        .saveAsTable(table_name)
    )


def write_rejected_delta_if_not_empty(df, rejected_output_path: str):
    """
    Writes rejected records to a Delta path only when invalid records exist.

    This avoids creating rejected folders with no records.
    """

    rejected_count = df.count()

    if rejected_count > 0:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"ingest_date = '{ingest_date}'")
            .partitionBy("ingest_date")
            .save(rejected_output_path)
        )

        print(f"Rejected records written: {rejected_count} -> {rejected_output_path}")
    else:
        print(f"No rejected records for path: {rejected_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Write silver Delta tables
# MAGIC
# MAGIC This step writes the clean and enriched silver DataFrames as Delta tables.
# MAGIC
# MAGIC The tables are written to explicit ADLS paths and registered in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Write silver Delta tables
write_delta_table_by_ingest_date(
    df=df_sensor_readings_clean_final,
    table_name=silver_sensor_readings_clean_table,
    table_path=silver_sensor_readings_clean_path
)

write_delta_table_by_ingest_date(
    df=df_sensor_metadata_clean_final,
    table_name=silver_sensor_metadata_clean_table,
    table_path=silver_sensor_metadata_clean_path
)

write_delta_table_by_ingest_date(
    df=df_sensor_readings_enriched,
    table_name=silver_sensor_readings_enriched_table,
    table_path=silver_sensor_readings_enriched_path
)

print("Silver Delta tables written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - Write rejected records
# MAGIC
# MAGIC This step writes invalid records to the rejected zone.
# MAGIC
# MAGIC Rejected records are separated by issue type and source dataset to keep the lake structure understandable.

# COMMAND ----------

# DBTITLE 1,Write rejected records
# Rejected output paths for sensor readings
rejected_sensor_readings_invalid_schema_path = f"{rejected_invalid_schema_path}sensor_readings/"
rejected_sensor_readings_null_sensor_id_path = f"{rejected_null_sensor_id_path}sensor_readings/"
rejected_sensor_readings_invalid_ranges_path = f"{rejected_invalid_ranges_path}sensor_readings/"

# Rejected output paths for sensor metadata
rejected_sensor_metadata_invalid_schema_path = f"{rejected_invalid_schema_path}sensor_metadata/"
rejected_sensor_metadata_null_sensor_id_path = f"{rejected_null_sensor_id_path}sensor_metadata/"
rejected_sensor_metadata_invalid_ranges_path = f"{rejected_invalid_ranges_path}sensor_metadata/"

write_rejected_delta_if_not_empty(
    df_sensor_readings_invalid_schema,
    rejected_sensor_readings_invalid_schema_path
)

write_rejected_delta_if_not_empty(
    df_sensor_readings_null_sensor_id,
    rejected_sensor_readings_null_sensor_id_path
)

write_rejected_delta_if_not_empty(
    df_sensor_readings_invalid_ranges,
    rejected_sensor_readings_invalid_ranges_path
)

write_rejected_delta_if_not_empty(
    df_sensor_metadata_invalid_schema,
    rejected_sensor_metadata_invalid_schema_path
)

write_rejected_delta_if_not_empty(
    df_sensor_metadata_null_sensor_id,
    rejected_sensor_metadata_null_sensor_id_path
)

write_rejected_delta_if_not_empty(
    df_sensor_metadata_invalid_values,
    rejected_sensor_metadata_invalid_ranges_path
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 - Validate silver tables after writing
# MAGIC
# MAGIC This step reads the silver Delta tables from Unity Catalog and validates the record counts for the selected ingestion date.

# COMMAND ----------

# DBTITLE 1,Validate silver tables after writing
df_sensor_readings_clean_check = (
    spark.table(silver_sensor_readings_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_sensor_metadata_clean_check = (
    spark.table(silver_sensor_metadata_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_sensor_readings_enriched_check = (
    spark.table(silver_sensor_readings_enriched_table)
    .filter(col("ingest_date") == ingest_date)
)

print(f"Silver sensor readings clean count: {df_sensor_readings_clean_check.count()}")
print(f"Silver sensor metadata clean count: {df_sensor_metadata_clean_check.count()}")
print(f"Silver sensor readings enriched count: {df_sensor_readings_enriched_check.count()}")

display(df_sensor_readings_clean_check.limit(10))
display(df_sensor_metadata_clean_check.limit(10))
display(df_sensor_readings_enriched_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17 - Display rejected summary
# MAGIC
# MAGIC This step displays a simple rejected record summary for the current ingestion date.
# MAGIC
# MAGIC The purpose is to quickly understand how many records were excluded and why.

# COMMAND ----------

# DBTITLE 1,Display rejected summary
rejected_summary = [
    ("sensor_readings", "invalid_schema", df_sensor_readings_invalid_schema.count()),
    ("sensor_readings", "null_sensor_id", df_sensor_readings_null_sensor_id.count()),
    ("sensor_readings", "invalid_ranges", df_sensor_readings_invalid_ranges.count()),
    ("sensor_metadata", "invalid_schema", df_sensor_metadata_invalid_schema.count()),
    ("sensor_metadata", "null_sensor_id", df_sensor_metadata_null_sensor_id.count()),
    ("sensor_metadata", "invalid_values", df_sensor_metadata_invalid_values.count())
]

df_rejected_summary = spark.createDataFrame(
    rejected_summary,
    ["dataset", "rejection_type", "record_count"]
)

display(df_rejected_summary)