# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01_landing_to_bronze
# MAGIC
# MAGIC This notebook loads the raw IoT Berkeley Lab source files from the landing layer and writes them as Delta tables in the bronze layer.
# MAGIC
# MAGIC Objective:
# MAGIC - Read the raw sensor readings file from landing.
# MAGIC - Read the raw sensor metadata file from landing.
# MAGIC - Preserve each source record as a raw text line.
# MAGIC - Add only useful technical metadata for traceability, reprocessing and duplicate detection.
# MAGIC - Write the results as Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC Inputs:
# MAGIC - landing/sensor_readings/ingest_date=YYYY-MM-DD/data.txt.gz
# MAGIC - landing/sensor_metadata/ingest_date=YYYY-MM-DD/mote_locs.txt
# MAGIC
# MAGIC Outputs:
# MAGIC - iot_berkeley_lab.bronze.sensor_readings_raw
# MAGIC - iot_berkeley_lab.bronze.sensor_metadata_raw
# MAGIC
# MAGIC Important:
# MAGIC The source files do not contain headers.
# MAGIC This notebook does not use header=true.
# MAGIC Column parsing is intentionally deferred to the silver layer.
# MAGIC
# MAGIC Processing flow:
# MAGIC 1. Load global configuration from 00_setup_config.
# MAGIC 2. Read sensor readings as raw text.
# MAGIC 3. Read sensor metadata as raw text.
# MAGIC 4. Add technical metadata with clear purpose.
# MAGIC 5. Validate that both bronze DataFrames contain records.
# MAGIC 6. Write Delta tables to the bronze layer.
# MAGIC 7. Display basic validation results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load shared project configuration
# MAGIC
# MAGIC This step imports the variables and helper functions defined in 00_setup_config.
# MAGIC
# MAGIC The configuration notebook provides the ADLS paths, Unity Catalog table names, selected ingestion date and expected source file paths.

# COMMAND ----------

# DBTITLE 1,Load shared project configuration
# MAGIC %run ./00_setup_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import required PySpark functions
# MAGIC
# MAGIC This step imports only the functions required in this notebook.
# MAGIC
# MAGIC The notebook uses these functions to clean empty lines, add technical metadata and generate a stable hash for each raw record.

# COMMAND ----------

# DBTITLE 1,Import required PySpark functions
from pyspark.sql.functions import col, current_timestamp, lit, sha2, concat_ws, trim

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define a reusable function to read raw text files
# MAGIC
# MAGIC This function reads a source file as plain text.
# MAGIC
# MAGIC Spark creates one column called value when reading text files.
# MAGIC The function renames that column to raw_line because each row represents one raw source record.
# MAGIC
# MAGIC Empty lines are removed because they do not represent valid source records and do not add analytical or audit value.

# COMMAND ----------

# DBTITLE 1,Define a reusable function to read raw text files
def read_raw_text_file(file_path: str):
    """
    Reads a raw text file and returns a DataFrame with one column: raw_line.

    Parameters:
    file_path: Full ADLS path of the source file.

    Returns:
    DataFrame with non-empty raw source lines.
    """

    df_raw = (
        spark.read
        .format("text")
        .load(file_path)
        .withColumnRenamed("value", "raw_line")
        .filter(trim(col("raw_line")) != "")
    )

    return df_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Read raw sensor readings from landing
# MAGIC
# MAGIC This step reads data.txt.gz from the selected ingestion date folder.
# MAGIC
# MAGIC The file is read as raw text.
# MAGIC No header option is used because the source file does not contain column names.
# MAGIC The gzip compression is handled automatically by Spark.

# COMMAND ----------

# DBTITLE 1,Read raw sensor readings from landing
df_sensor_readings_raw = read_raw_text_file(sensor_readings_file_path)

display(df_sensor_readings_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Read raw sensor metadata from landing
# MAGIC
# MAGIC This step reads mote_locs.txt from the selected ingestion date folder.
# MAGIC
# MAGIC The file is also read as raw text.
# MAGIC Column parsing will be done later in the silver layer.

# COMMAND ----------

# DBTITLE 1,Read raw sensor metadata from landing
df_sensor_metadata_raw = read_raw_text_file(sensor_metadata_file_path)

display(df_sensor_metadata_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Add technical metadata to bronze records
# MAGIC
# MAGIC This step adds only technical columns with a clear purpose.
# MAGIC
# MAGIC Columns added:
# MAGIC - ingest_date: identifies the ADF ingestion folder and supports partitioned reprocessing.
# MAGIC - source_file_path: keeps lineage to the original file in ADLS.
# MAGIC - ingestion_timestamp: records when Databricks processed the file.
# MAGIC - record_hash: creates a stable fingerprint for duplicate detection and idempotent processing.
# MAGIC
# MAGIC The source file name is not added because it is already contained in source_file_path.

# COMMAND ----------

# DBTITLE 1,Add technical metadata to bronze records
def add_bronze_metadata(df, source_file_path: str):
    """
    Adds useful technical metadata to a bronze DataFrame.

    Parameters:
    df: Raw DataFrame with raw_line column.
    source_file_path: Full ADLS path of the original source file.

    Returns:
    DataFrame with technical metadata columns.
    """

    df_with_metadata = (
        df
        .withColumn("ingest_date", lit(ingest_date))
        .withColumn("source_file_path", lit(source_file_path))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("raw_line"),
                    col("ingest_date"),
                    col("source_file_path")
                ),
                256
            )
        )
    )

    return df_with_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create bronze DataFrames
# MAGIC
# MAGIC This step creates the final bronze DataFrames for sensor readings and sensor metadata.
# MAGIC
# MAGIC Both DataFrames keep the same minimal structure:
# MAGIC - raw_line
# MAGIC - ingest_date
# MAGIC - source_file_path
# MAGIC - ingestion_timestamp
# MAGIC - record_hash

# COMMAND ----------

# DBTITLE 1,Create bronze DataFrames
df_bronze_sensor_readings = add_bronze_metadata(
    df=df_sensor_readings_raw,
    source_file_path=sensor_readings_file_path
)

df_bronze_sensor_metadata = add_bronze_metadata(
    df=df_sensor_metadata_raw,
    source_file_path=sensor_metadata_file_path
)

display(df_bronze_sensor_readings.limit(10))
display(df_bronze_sensor_metadata.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate bronze record counts before writing
# MAGIC
# MAGIC This step validates that both source files produced records.
# MAGIC
# MAGIC If a DataFrame has zero records, the notebook fails before writing empty Delta tables.

# COMMAND ----------

# DBTITLE 1,Validate bronze record counts before writing
sensor_readings_count = df_bronze_sensor_readings.count()
sensor_metadata_count = df_bronze_sensor_metadata.count()

print(f"Sensor readings raw record count: {sensor_readings_count}")
print(f"Sensor metadata raw record count: {sensor_metadata_count}")

if sensor_readings_count == 0:
    raise ValueError("Sensor readings raw DataFrame is empty. Bronze write stopped.")

if sensor_metadata_count == 0:
    raise ValueError("Sensor metadata raw DataFrame is empty. Bronze write stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Write bronze Delta tables
# MAGIC
# MAGIC This step writes the raw bronze DataFrames as Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC The tables are written with explicit ADLS paths to keep the physical lakehouse structure aligned with the project architecture.
# MAGIC
# MAGIC The write mode uses overwrite with replaceWhere on ingest_date.
# MAGIC This prevents duplicate records when the same ingestion date is reprocessed.

# COMMAND ----------

# DBTITLE 1,Write bronze Delta tables
(
    df_bronze_sensor_readings.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"ingest_date = '{ingest_date}'")
    .option("path", bronze_sensor_readings_path)
    .partitionBy("ingest_date")
    .saveAsTable(bronze_sensor_readings_table)
)

(
    df_bronze_sensor_metadata.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"ingest_date = '{ingest_date}'")
    .option("path", bronze_sensor_metadata_path)
    .partitionBy("ingest_date")
    .saveAsTable(bronze_sensor_metadata_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Validate bronze tables after writing
# MAGIC
# MAGIC This step reads the bronze Delta tables from Unity Catalog and validates that the selected ingestion date was written successfully.

# COMMAND ----------

# DBTITLE 1,Validate bronze tables after writing
df_bronze_sensor_readings_check = spark.table(bronze_sensor_readings_table).filter(
    col("ingest_date") == ingest_date
)

df_bronze_sensor_metadata_check = spark.table(bronze_sensor_metadata_table).filter(
    col("ingest_date") == ingest_date
)

bronze_sensor_readings_written_count = df_bronze_sensor_readings_check.count()
bronze_sensor_metadata_written_count = df_bronze_sensor_metadata_check.count()

print(f"Bronze sensor readings records written for {ingest_date}: {bronze_sensor_readings_written_count}")
print(f"Bronze sensor metadata records written for {ingest_date}: {bronze_sensor_metadata_written_count}")

display(df_bronze_sensor_readings_check.limit(10))
display(df_bronze_sensor_metadata_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Describe created Delta tables
# MAGIC
# MAGIC This step displays Delta table metadata for both bronze tables.
# MAGIC
# MAGIC This confirms that the tables are registered in Unity Catalog and physically stored in the expected ADLS bronze paths.

# COMMAND ----------

# DBTITLE 1,Describe created Delta tables
display(spark.sql(f"DESCRIBE DETAIL {bronze_sensor_readings_table}"))
display(spark.sql(f"DESCRIBE DETAIL {bronze_sensor_metadata_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC