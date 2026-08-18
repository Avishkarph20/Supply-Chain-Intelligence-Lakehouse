# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04_silver_to_gold_facts
# MAGIC
# MAGIC This notebook creates the Gold fact tables for the IoT Berkeley Lab Analytics Platform.
# MAGIC
# MAGIC Objective:
# MAGIC - Create a detailed sensor reading fact table.
# MAGIC - Create hourly aggregated sensor metrics.
# MAGIC - Create daily aggregated sensor metrics.
# MAGIC - Create sensor health metrics focused on voltage and reading availability.
# MAGIC - Write all fact tables as Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC Inputs:
# MAGIC - iot_berkeley_lab.silver.sensor_readings_clean
# MAGIC - iot_berkeley_lab.silver.sensor_readings_enriched
# MAGIC - iot_berkeley_lab.gold.dim_date
# MAGIC - iot_berkeley_lab.gold.dim_time
# MAGIC - iot_berkeley_lab.gold.dim_mote
# MAGIC
# MAGIC Outputs:
# MAGIC - iot_berkeley_lab.gold.fact_sensor_reading
# MAGIC - iot_berkeley_lab.gold.fact_sensor_hourly
# MAGIC - iot_berkeley_lab.gold.fact_sensor_daily
# MAGIC - iot_berkeley_lab.gold.fact_sensor_health
# MAGIC
# MAGIC Processing flow:
# MAGIC 1. Load shared configuration.
# MAGIC 2. Read Silver and Gold dimension tables.
# MAGIC 3. Define the low voltage threshold.
# MAGIC 4. Create fact_sensor_reading at reading-level granularity.
# MAGIC 5. Create fact_sensor_hourly at date-hour granularity.
# MAGIC 6. Create fact_sensor_daily at date granularity.
# MAGIC 7. Create fact_sensor_health at sensor-date granularity.
# MAGIC 8. Validate fact keys and row counts.
# MAGIC 9. Write Gold fact Delta tables.
# MAGIC 10. Display final validation results.
# MAGIC
# MAGIC Important:
# MAGIC Only columns with analytical, relationship, reprocessing or health-monitoring value are created.
# MAGIC Gold tables are overwritten completely from the selected clean Silver batch to avoid duplicated analytical results if the static source is reprocessed.

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
# MAGIC This step imports the functions required to create fact keys, aggregate sensor measurements and validate the output.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import (
    col,
    lit,
    when,
    date_format,
    hour,
    minute,
    count,
    countDistinct,
    avg,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    first,
    last
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read Silver and Gold dimension tables
# MAGIC
# MAGIC This step reads the clean Silver data for the selected ingestion date.
# MAGIC
# MAGIC The dimension tables are also read to validate that fact foreign keys can connect to the Gold dimensional model.

# COMMAND ----------

# DBTITLE 1,Read Silver and Gold dimension tables
df_sensor_readings_clean = (
    spark.table(silver_sensor_readings_clean_table)
    .filter(col("ingest_date") == ingest_date)
)

df_sensor_readings_enriched = (
    spark.table(silver_sensor_readings_enriched_table)
    .filter(col("ingest_date") == ingest_date)
)

df_dim_date = spark.table(gold_dim_date_table)
df_dim_time = spark.table(gold_dim_time_table)
df_dim_mote = spark.table(gold_dim_mote_table)

print(f"Silver sensor readings clean count: {df_sensor_readings_clean.count()}")
print(f"Silver sensor readings enriched count: {df_sensor_readings_enriched.count()}")
print(f"Gold dim_date count: {df_dim_date.count()}")
print(f"Gold dim_time count: {df_dim_time.count()}")
print(f"Gold dim_mote count: {df_dim_mote.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Define low voltage threshold
# MAGIC
# MAGIC This step defines the voltage threshold used to identify possible low battery behavior.
# MAGIC
# MAGIC The value is centralized in one variable so it can be adjusted later without rewriting the aggregation logic.

# COMMAND ----------

# DBTITLE 1,Define low voltage threshold
low_voltage_threshold = 2.4

print(f"Low voltage threshold selected: {low_voltage_threshold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Create fact_sensor_reading
# MAGIC
# MAGIC This step creates the detailed fact table.
# MAGIC
# MAGIC The granularity is one row per valid sensor reading.
# MAGIC
# MAGIC This table keeps the exact event timestamp and the numeric sensor measurements needed for detailed analysis.

# COMMAND ----------

# DBTITLE 1,Create fact_sensor_reading
df_fact_sensor_reading = (
    df_sensor_readings_clean
    .withColumn("date_key", date_format(col("event_date"), "yyyyMMdd").cast("int"))
    .withColumn(
        "time_key",
        (hour(col("event_timestamp")) * 100 + minute(col("event_timestamp"))).cast("int")
    )
    .select(
        col("reading_id"),
        col("date_key"),
        col("time_key"),
        col("mote_id"),
        col("event_timestamp"),
        col("epoch"),
        col("temperature_celsius"),
        col("humidity_percent"),
        col("light"),
        col("voltage"),
        col("ingest_date")
    )
)

display(df_fact_sensor_reading.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Create fact_sensor_hourly
# MAGIC
# MAGIC This step creates hourly aggregated sensor metrics.
# MAGIC
# MAGIC The granularity is one row per date and hour.
# MAGIC
# MAGIC This fact is useful for analyzing how temperature, humidity, light and voltage behave throughout the day.

# COMMAND ----------

# DBTITLE 1,Create fact_sensor_hourly
df_fact_sensor_hourly = (
    df_fact_sensor_reading
    .withColumn("event_hour", hour(col("event_timestamp")))
    .groupBy(
        col("date_key"),
        col("event_hour"),
        col("ingest_date")
    )
    .agg(
        count("*").alias("reading_count"),
        countDistinct("mote_id").alias("active_sensor_count"),
        avg("temperature_celsius").alias("avg_temperature_celsius"),
        spark_min("temperature_celsius").alias("min_temperature_celsius"),
        spark_max("temperature_celsius").alias("max_temperature_celsius"),
        avg("humidity_percent").alias("avg_humidity_percent"),
        avg("light").alias("avg_light"),
        avg("voltage").alias("avg_voltage"),
        spark_min("voltage").alias("min_voltage"),
        spark_sum(
            when(col("voltage") < lit(low_voltage_threshold), lit(1)).otherwise(lit(0))
        ).alias("low_voltage_reading_count")
    )
    .select(
        col("date_key"),
        col("event_hour"),
        col("reading_count"),
        col("active_sensor_count"),
        col("avg_temperature_celsius"),
        col("min_temperature_celsius"),
        col("max_temperature_celsius"),
        col("avg_humidity_percent"),
        col("avg_light"),
        col("avg_voltage"),
        col("min_voltage"),
        col("low_voltage_reading_count"),
        col("ingest_date")
    )
    .orderBy("date_key", "event_hour")
)

display(df_fact_sensor_hourly.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create fact_sensor_daily
# MAGIC
# MAGIC This step creates daily aggregated sensor metrics.
# MAGIC
# MAGIC The granularity is one row per date.
# MAGIC
# MAGIC This fact is useful for dashboards and high-level trends.

# COMMAND ----------

# DBTITLE 1,Create fact_sensor_daily
df_fact_sensor_daily = (
    df_fact_sensor_reading
    .groupBy(
        col("date_key"),
        col("ingest_date")
    )
    .agg(
        count("*").alias("reading_count"),
        countDistinct("mote_id").alias("active_sensor_count"),
        avg("temperature_celsius").alias("avg_temperature_celsius"),
        spark_min("temperature_celsius").alias("min_temperature_celsius"),
        spark_max("temperature_celsius").alias("max_temperature_celsius"),
        avg("humidity_percent").alias("avg_humidity_percent"),
        avg("light").alias("avg_light"),
        avg("voltage").alias("avg_voltage"),
        spark_min("voltage").alias("min_voltage"),
        spark_sum(
            when(col("voltage") < lit(low_voltage_threshold), lit(1)).otherwise(lit(0))
        ).alias("low_voltage_reading_count")
    )
    .select(
        col("date_key"),
        col("reading_count"),
        col("active_sensor_count"),
        col("avg_temperature_celsius"),
        col("min_temperature_celsius"),
        col("max_temperature_celsius"),
        col("avg_humidity_percent"),
        col("avg_light"),
        col("avg_voltage"),
        col("min_voltage"),
        col("low_voltage_reading_count"),
        col("ingest_date")
    )
    .orderBy("date_key")
)

display(df_fact_sensor_daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Create fact_sensor_health
# MAGIC
# MAGIC This step creates daily health metrics by sensor.
# MAGIC
# MAGIC The granularity is one row per sensor and date.
# MAGIC
# MAGIC This fact focuses on voltage behavior and reading availability, which are useful signals for detecting possible sensor or battery issues.

# COMMAND ----------

# DBTITLE 1,Create fact_sensor_health
df_fact_sensor_health = (
    df_fact_sensor_reading
    .groupBy(
        col("date_key"),
        col("mote_id"),
        col("ingest_date")
    )
    .agg(
        count("*").alias("reading_count"),
        spark_min("event_timestamp").alias("first_reading_timestamp"),
        spark_max("event_timestamp").alias("last_reading_timestamp"),
        avg("voltage").alias("avg_voltage"),
        spark_min("voltage").alias("min_voltage"),
        spark_sum(
            when(col("voltage") < lit(low_voltage_threshold), lit(1)).otherwise(lit(0))
        ).alias("low_voltage_reading_count")
    )
    .withColumn(
        "low_voltage_detected",
        when(col("low_voltage_reading_count") > 0, lit(True)).otherwise(lit(False))
    )
    .select(
        col("date_key"),
        col("mote_id"),
        col("reading_count"),
        col("first_reading_timestamp"),
        col("last_reading_timestamp"),
        col("avg_voltage"),
        col("min_voltage"),
        col("low_voltage_reading_count"),
        col("low_voltage_detected"),
        col("ingest_date")
    )
    .orderBy("date_key", "mote_id")
)

display(df_fact_sensor_health.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate fact table keys and counts
# MAGIC
# MAGIC This step validates that the fact tables have usable relationship keys.
# MAGIC
# MAGIC The detailed fact also validates that reading_id is unique.

# COMMAND ----------

# DBTITLE 1,Validate fact table keys and counts
def validate_no_nulls(df, columns: list, dataframe_name: str):
    """
    Validates that important relationship columns do not contain null values.
    """

    for column_name in columns:
        null_count = df.filter(col(column_name).isNull()).count()
        print(f"{dataframe_name} null {column_name} count: {null_count}")

        if null_count > 0:
            raise ValueError(f"{dataframe_name} contains null values in {column_name}")


def validate_unique_key(df, key_column: str, dataframe_name: str):
    """
    Validates that a key column uniquely identifies rows.
    """

    duplicate_count = (
        df.groupBy(key_column)
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .count()
    )

    print(f"{dataframe_name} duplicate {key_column} count: {duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(f"{dataframe_name} contains duplicate values in {key_column}")


validate_no_nulls(
    df_fact_sensor_reading,
    ["reading_id", "date_key", "time_key", "mote_id"],
    "fact_sensor_reading"
)

validate_unique_key(
    df_fact_sensor_reading,
    "reading_id",
    "fact_sensor_reading"
)

validate_no_nulls(
    df_fact_sensor_hourly,
    ["date_key", "event_hour"],
    "fact_sensor_hourly"
)

validate_no_nulls(
    df_fact_sensor_daily,
    ["date_key"],
    "fact_sensor_daily"
)

validate_no_nulls(
    df_fact_sensor_health,
    ["date_key", "mote_id"],
    "fact_sensor_health"
)

print(f"fact_sensor_reading count: {df_fact_sensor_reading.count()}")
print(f"fact_sensor_hourly count: {df_fact_sensor_hourly.count()}")
print(f"fact_sensor_daily count: {df_fact_sensor_daily.count()}")
print(f"fact_sensor_health count: {df_fact_sensor_health.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Validate fact relationships with dimensions
# MAGIC
# MAGIC This step checks that fact relationship keys exist in the Gold dimensions.
# MAGIC
# MAGIC This helps confirm that the star schema is consistent before writing the final fact tables.

# COMMAND ----------

# DBTITLE 1,Validate fact relationships with dimensions
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

print(f"Missing date keys in dim_date: {missing_date_keys_count}")
print(f"Missing time keys in dim_time: {missing_time_keys_count}")
print(f"Missing mote ids in dim_mote: {missing_mote_ids_count}")

if missing_date_keys_count > 0:
    raise ValueError("Some fact date_key values do not exist in dim_date.")

if missing_time_keys_count > 0:
    raise ValueError("Some fact time_key values do not exist in dim_time.")

if missing_mote_ids_count > 0:
    raise ValueError("Some fact mote_id values do not exist in dim_mote.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Define reusable function to write Gold facts
# MAGIC
# MAGIC This step defines a helper function to write Gold fact tables as Delta tables.
# MAGIC
# MAGIC The tables are fully overwritten because the source is static and the Gold layer should represent the latest clean analytical version of the selected batch.

# COMMAND ----------

# DBTITLE 1,Define reusable function to write Gold facts
def write_gold_fact(df, table_name: str, table_path: str, partition_columns: list = None):
    """
    Writes a Gold fact table as a Delta table registered in Unity Catalog.

    Parameters:
    df: DataFrame to write.
    table_name: Fully qualified Unity Catalog table name.
    table_path: Physical ADLS path.
    partition_columns: Optional list of columns used for physical partitioning.
    """

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", table_path)
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Write Gold fact tables
# MAGIC
# MAGIC This step writes all Gold fact tables to explicit ADLS paths and registers them in Unity Catalog.
# MAGIC
# MAGIC The detailed fact and aggregated facts are partitioned by date_key because most analytical queries will filter or group by date.

# COMMAND ----------

# DBTITLE 1,Write Gold fact tables
write_gold_fact(
    df=df_fact_sensor_reading,
    table_name=gold_fact_sensor_reading_table,
    table_path=gold_fact_sensor_reading_path,
    partition_columns=["date_key"]
)

write_gold_fact(
    df=df_fact_sensor_hourly,
    table_name=gold_fact_sensor_hourly_table,
    table_path=gold_fact_sensor_hourly_path,
    partition_columns=["date_key"]
)

write_gold_fact(
    df=df_fact_sensor_daily,
    table_name=gold_fact_sensor_daily_table,
    table_path=gold_fact_sensor_daily_path,
    partition_columns=["date_key"]
)

write_gold_fact(
    df=df_fact_sensor_health,
    table_name=gold_fact_sensor_health_table,
    table_path=gold_fact_sensor_health_path,
    partition_columns=["date_key"]
)

print("Gold fact tables written successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Validate Gold facts after writing
# MAGIC
# MAGIC This step reads the Gold facts from Unity Catalog and validates their final row counts.

# COMMAND ----------

# DBTITLE 1,Validate Gold facts after writing
df_fact_sensor_reading_check = spark.table(gold_fact_sensor_reading_table)
df_fact_sensor_hourly_check = spark.table(gold_fact_sensor_hourly_table)
df_fact_sensor_daily_check = spark.table(gold_fact_sensor_daily_table)
df_fact_sensor_health_check = spark.table(gold_fact_sensor_health_table)

print(f"gold.fact_sensor_reading count: {df_fact_sensor_reading_check.count()}")
print(f"gold.fact_sensor_hourly count: {df_fact_sensor_hourly_check.count()}")
print(f"gold.fact_sensor_daily count: {df_fact_sensor_daily_check.count()}")
print(f"gold.fact_sensor_health count: {df_fact_sensor_health_check.count()}")

display(df_fact_sensor_reading_check.limit(10))
display(df_fact_sensor_hourly_check.limit(20))
display(df_fact_sensor_daily_check)
display(df_fact_sensor_health_check.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Describe Gold fact Delta tables
# MAGIC
# MAGIC This step displays Delta table metadata for the created fact tables.
# MAGIC
# MAGIC It confirms that each table is registered in Unity Catalog and physically stored in the expected Gold ADLS path.

# COMMAND ----------

# DBTITLE 1,Describe Gold fact Delta tables
display(spark.sql(f"DESCRIBE DETAIL {gold_fact_sensor_reading_table}"))
display(spark.sql(f"DESCRIBE DETAIL {gold_fact_sensor_hourly_table}"))
display(spark.sql(f"DESCRIBE DETAIL {gold_fact_sensor_daily_table}"))
display(spark.sql(f"DESCRIBE DETAIL {gold_fact_sensor_health_table}"))