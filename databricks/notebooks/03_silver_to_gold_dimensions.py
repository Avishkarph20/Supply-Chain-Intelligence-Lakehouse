# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03_silver_to_gold_dimensions
# MAGIC
# MAGIC This notebook creates the Gold dimension tables for the IoT Berkeley Lab Analytics Platform.
# MAGIC
# MAGIC Objective:
# MAGIC - Create a sensor dimension from clean sensor metadata and observed sensor readings.
# MAGIC - Create a date dimension from clean sensor reading event dates.
# MAGIC - Create a time dimension from clean sensor reading timestamps.
# MAGIC - Write the dimensions as Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC Inputs:
# MAGIC - iot_berkeley_lab.silver.sensor_readings_clean
# MAGIC - iot_berkeley_lab.silver.sensor_metadata_clean
# MAGIC - iot_berkeley_lab.silver.sensor_readings_enriched
# MAGIC
# MAGIC Outputs:
# MAGIC - iot_berkeley_lab.gold.dim_mote
# MAGIC - iot_berkeley_lab.gold.dim_date
# MAGIC - iot_berkeley_lab.gold.dim_time
# MAGIC
# MAGIC Processing flow:
# MAGIC 1. Load shared configuration.
# MAGIC 2. Read Silver tables for the selected ingestion date.
# MAGIC 3. Create dim_mote.
# MAGIC 4. Create dim_date.
# MAGIC 5. Create dim_time.
# MAGIC 6. Validate dimension keys.
# MAGIC 7. Write Gold dimension Delta tables.
# MAGIC 8. Display final validation results.
# MAGIC
# MAGIC Important:
# MAGIC Only columns with analytical, relationship or quality value are created.
# MAGIC No operational logs are created in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load shared project configuration
# MAGIC
# MAGIC This step imports the global configuration from 00_setup_config.
# MAGIC
# MAGIC The configuration provides the ADLS paths, Unity Catalog table names and selected ingestion date.

# COMMAND ----------

# DBTITLE 1,Load shared project configuration
# MAGIC %run ./00_setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import required PySpark functions
# MAGIC
# MAGIC This step imports the functions required to build date, time and sensor dimensions.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import (
    col,
    lit,
    when,
    date_format,
    to_timestamp,
    concat_ws,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    hour,
    minute,
    count
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read Silver tables for the selected ingestion date
# MAGIC
# MAGIC This step reads the clean Silver tables for the selected ingestion date.
# MAGIC
# MAGIC The Gold layer will be rebuilt from the selected clean batch to avoid duplicated analytical results if the same static source is ingested more than once.

# COMMAND ----------

# DBTITLE 1,Read Silver tables for the selected ingestion date
df_sensor_readings_clean = (
    spark.table(silver_sensor_readings_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_sensor_metadata_clean = (
    spark.table(silver_sensor_metadata_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_sensor_readings_enriched = (
    spark.table(silver_sensor_readings_enriched_table)
    .filter(col("ingest_date") == ingest_date)
)

print(f"Silver sensor readings clean count: {df_sensor_readings_clean.count()}")
print(f"Silver sensor metadata clean count: {df_sensor_metadata_clean.count()}")
print(f"Silver sensor readings enriched count: {df_sensor_readings_enriched.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Create dim_mote
# MAGIC
# MAGIC This step creates the sensor dimension.
# MAGIC
# MAGIC The dimension includes all sensors found in metadata and all sensors observed in readings.
# MAGIC
# MAGIC This prevents losing sensors that appear in readings but do not have metadata.

# COMMAND ----------

# DBTITLE 1,Create dim_mote
df_mote_ids_from_readings = (
    df_sensor_readings_clean
    .select("mote_id")
    .where(col("mote_id").isNotNull())
    .distinct()
)

df_mote_ids_from_metadata = (
    df_sensor_metadata_clean
    .select("mote_id")
    .where(col("mote_id").isNotNull())
    .distinct()
)

df_all_mote_ids = (
    df_mote_ids_from_readings
    .unionByName(df_mote_ids_from_metadata)
    .dropDuplicates(["mote_id"])
)

df_dim_mote = (
    df_all_mote_ids.alias("s")
    .join(
        df_sensor_metadata_clean
        .select("mote_id", "x_position", "y_position")
        .dropDuplicates(["mote_id"])
        .alias("m"),
        on="mote_id",
        how="left"
    )
    .withColumn(
        "metadata_available",
        when(
            col("x_position").isNotNull() & col("y_position").isNotNull(),
            lit(True)
        ).otherwise(lit(False))
    )
    .select(
        col("mote_id"),
        col("x_position"),
        col("y_position"),
        col("metadata_available")
    )
    .orderBy("mote_id")
)

display(df_dim_mote)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Create dim_date
# MAGIC
# MAGIC This step creates the date dimension from distinct event dates found in clean sensor readings.
# MAGIC
# MAGIC The date_key uses yyyyMMdd format as an integer, which is a common pattern for dimensional models.

# COMMAND ----------

# DBTITLE 1,Create dim_date
df_dim_date = (
    df_sensor_readings_clean
    .select("event_date")
    .where(col("event_date").isNotNull())
    .distinct()
    .withColumn("date_key", date_format(col("event_date"), "yyyyMMdd").cast("int"))
    .withColumnRenamed("event_date", "full_date")
    .withColumn("calendar_year", year(col("full_date")))
    .withColumn("calendar_quarter", quarter(col("full_date")))
    .withColumn("month_number", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("day_of_month", dayofmonth(col("full_date")))
    .withColumn("day_of_week_number", dayofweek(col("full_date")))
    .withColumn("day_name", date_format(col("full_date"), "EEEE"))
    .withColumn(
        "is_weekend",
        when(dayofweek(col("full_date")).isin(1, 7), lit(True)).otherwise(lit(False))
    )
    .select(
        col("date_key"),
        col("full_date"),
        col("calendar_year"),
        col("calendar_quarter"),
        col("month_number"),
        col("month_name"),
        col("day_of_month"),
        col("day_of_week_number"),
        col("day_name"),
        col("is_weekend")
    )
    .orderBy("full_date")
)

display(df_dim_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Create dim_time
# MAGIC
# MAGIC This step creates the time dimension at minute granularity.
# MAGIC
# MAGIC The original event timestamp remains available in the fact table, so this dimension does not need second or microsecond granularity.

# COMMAND ----------

# DBTITLE 1,Create dim_time
df_time_base = (
    df_sensor_readings_clean
    .select("event_timestamp")
    .where(col("event_timestamp").isNotNull())
    .withColumn("event_hour", hour(col("event_timestamp")))
    .withColumn("event_minute", minute(col("event_timestamp")))
    .select("event_hour", "event_minute")
    .distinct()
)

df_dim_time = (
    df_time_base
    .withColumn(
        "time_key",
        (col("event_hour") * 100 + col("event_minute")).cast("int")
    )
    .withColumn(
        "time_label",
        date_format(
            to_timestamp(
                concat_ws(
                    ":",
                    col("event_hour").cast("string"),
                    col("event_minute").cast("string")
                ),
                "H:m"
            ),
            "HH:mm"
        )
    )
    .withColumn(
        "day_period",
        when((col("event_hour") >= 0) & (col("event_hour") < 6), "Night")
        .when((col("event_hour") >= 6) & (col("event_hour") < 12), "Morning")
        .when((col("event_hour") >= 12) & (col("event_hour") < 18), "Afternoon")
        .otherwise("Evening")
    )
    .select(
        col("time_key"),
        col("event_hour"),
        col("event_minute"),
        col("time_label"),
        col("day_period")
    )
    .orderBy("time_key")
)

display(df_dim_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Validate dimension keys
# MAGIC
# MAGIC This step validates that dimension keys are not null and are unique.
# MAGIC
# MAGIC A dimension key must uniquely identify each dimension row.

# COMMAND ----------

# DBTITLE 1,Validate dimension keys
def validate_unique_key(df, key_column: str, dimension_name: str):
    """
    Validates that a dimension key is not null and unique.
    """

    null_key_count = df.filter(col(key_column).isNull()).count()

    duplicate_key_count = (
        df.groupBy(key_column)
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .count()
    )

    print(f"{dimension_name} null key count: {null_key_count}")
    print(f"{dimension_name} duplicate key count: {duplicate_key_count}")

    if null_key_count > 0:
        raise ValueError(f"{dimension_name} has null values in key column: {key_column}")

    if duplicate_key_count > 0:
        raise ValueError(f"{dimension_name} has duplicate values in key column: {key_column}")


validate_unique_key(df_dim_mote, "mote_id", "dim_mote")
validate_unique_key(df_dim_date, "date_key", "dim_date")
validate_unique_key(df_dim_time, "time_key", "dim_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Define reusable function to write Gold dimensions
# MAGIC
# MAGIC This step defines a helper function to write a Gold dimension as a Delta table registered in Unity Catalog.
# MAGIC
# MAGIC Dimensions are overwritten completely because they are rebuilt from the selected clean Silver dataset.

# COMMAND ----------

# DBTITLE 1,Define reusable function to write Gold dimensions
def write_gold_dimension(df, table_name: str, table_path: str):
    """
    Writes a Gold dimension as a Delta table registered in Unity Catalog.

    The table is fully overwritten because dimensions are rebuilt from the selected clean dataset.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_path)
        .saveAsTable(table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Write Gold dimension tables
# MAGIC
# MAGIC This step writes the final Gold dimensions to explicit ADLS paths and registers them in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Write Gold dimension tables
write_gold_dimension(
    df=df_dim_mote,
    table_name=gold_dim_mote_table,
    table_path=gold_dim_mote_path
)

write_gold_dimension(
    df=df_dim_date,
    table_name=gold_dim_date_table,
    table_path=gold_dim_date_path
)

write_gold_dimension(
    df=df_dim_time,
    table_name=gold_dim_time_table,
    table_path=gold_dim_time_path
)

print("Gold dimension tables written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Validate Gold dimensions after writing
# MAGIC
# MAGIC This step reads the Gold dimensions from Unity Catalog and validates the final row counts.

# COMMAND ----------

# DBTITLE 1,Validate Gold dimensions after writing
df_dim_mote_check = spark.table(gold_dim_mote_table)
df_dim_date_check = spark.table(gold_dim_date_table)
df_dim_time_check = spark.table(gold_dim_time_table)

print(f"gold.dim_mote count: {df_dim_mote_check.count()}")
print(f"gold.dim_date count: {df_dim_date_check.count()}")
print(f"gold.dim_time count: {df_dim_time_check.count()}")

display(df_dim_mote_check)
display(df_dim_date_check)
display(df_dim_time_check.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Describe Gold dimension Delta tables
# MAGIC
# MAGIC This step displays Delta table metadata.
# MAGIC
# MAGIC It confirms that each Gold dimension is registered in Unity Catalog and stored in the expected ADLS Gold path.

# COMMAND ----------

# DBTITLE 1,Describe Gold dimension Delta tables
display(spark.sql(f"DESCRIBE DETAIL {gold_dim_mote_table}"))
display(spark.sql(f"DESCRIBE DETAIL {gold_dim_date_table}"))
display(spark.sql(f"DESCRIBE DETAIL {gold_dim_time_table}"))