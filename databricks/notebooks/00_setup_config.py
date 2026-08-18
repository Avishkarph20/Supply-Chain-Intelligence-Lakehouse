# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 00_setup_config
# MAGIC
# MAGIC This notebook centralizes the global configuration for the IoT Berkeley Lab Analytics Platform.
# MAGIC
# MAGIC The purpose of this notebook is to define reusable variables and helper functions that will be used by the processing notebooks.
# MAGIC
# MAGIC Inputs:
# MAGIC - ADLS Gen2 base path for the IoT Berkeley Lab project.
# MAGIC - Unity Catalog catalog name.
# MAGIC - Unity Catalog schemas for bronze, silver and gold layers.
# MAGIC - Optional ingestion date parameter.
# MAGIC
# MAGIC Outputs:
# MAGIC - Reusable Python variables for landing, bronze, silver and gold paths.
# MAGIC - Reusable table names for Unity Catalog.
# MAGIC - A resolved ingestion date.
# MAGIC - Helper functions to inspect landing folders and resolve the latest available ingestion date.
# MAGIC
# MAGIC Processing flow:
# MAGIC 1. Define project-level constants.
# MAGIC 2. Define Unity Catalog objects.
# MAGIC 3. Define ADLS Gen2 paths.
# MAGIC 4. Create an optional ingestion date widget.
# MAGIC 5. Resolve the ingestion date to process.
# MAGIC 6. Validate that landing folders exist.
# MAGIC 7. Display the final configuration summary.
# MAGIC
# MAGIC Important:
# MAGIC The source files do not contain headers. Downstream notebooks must assign column names manually and must not use header=true.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Define project-level constants
# MAGIC
# MAGIC This step defines the core project variables, including the storage account, container, base ADLS path and Unity Catalog catalog name.
# MAGIC
# MAGIC These variables are reused across all notebooks to avoid hardcoding paths and names multiple times.

# COMMAND ----------

# DBTITLE 1,Define project-level constants
# Project identifiers
project_name = "iot_berkeley_lab"

# ADLS Gen2 configuration
storage_account_name = "saiotberkeley"
container_name = "iot-berkeley-lab"

# Root path of the project in ADLS Gen2
base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Unity Catalog configuration
catalog_name = "iot_berkeley_lab"

# Layer schemas
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Define ADLS paths by lakehouse layer
# MAGIC
# MAGIC This step defines the physical ADLS paths for the landing, bronze, silver and gold layers.
# MAGIC
# MAGIC The landing layer contains the files copied by Azure Data Factory.
# MAGIC The bronze layer will store raw Delta tables with technical metadata.
# MAGIC The silver layer will store cleaned, typed and validated data.
# MAGIC The gold layer will store analytical dimensional and fact tables.

# COMMAND ----------

# DBTITLE 1,Define ADLS paths by lakehouse layer
# Landing paths
landing_path = f"{base_path}landing/"

landing_sensor_readings_root = f"{landing_path}sensor_readings/"
landing_sensor_metadata_root = f"{landing_path}sensor_metadata/"

# Bronze paths
bronze_path = f"{base_path}bronze/"

bronze_sensor_readings_path = f"{bronze_path}sensor_readings/"
bronze_sensor_metadata_path = f"{bronze_path}sensor_metadata/"

# Silver paths
silver_path = f"{base_path}silver/"

silver_sensor_readings_clean_path = f"{silver_path}sensor_readings_clean/"
silver_sensor_metadata_clean_path = f"{silver_path}sensor_metadata_clean/"
silver_sensor_readings_enriched_path = f"{silver_path}sensor_readings_enriched/"

# Gold paths
gold_path = f"{base_path}gold/"

gold_dim_mote_path = f"{gold_path}dim_mote/"
gold_dim_date_path = f"{gold_path}dim_date/"
gold_dim_time_path = f"{gold_path}dim_time/"

gold_fact_sensor_reading_path = f"{gold_path}fact_sensor_reading/"
gold_fact_sensor_hourly_path = f"{gold_path}fact_sensor_hourly/"
gold_fact_sensor_daily_path = f"{gold_path}fact_sensor_daily/"
gold_fact_sensor_health_path = f"{gold_path}fact_sensor_health/"

# Rejected paths
rejected_path = f"{base_path}rejected/"

rejected_invalid_schema_path = f"{rejected_path}invalid_schema/"
rejected_null_sensor_id_path = f"{rejected_path}null_sensor_id/"
rejected_invalid_ranges_path = f"{rejected_path}invalid_ranges/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define Unity Catalog table names
# MAGIC
# MAGIC This step defines the full Unity Catalog table names that will be created in later notebooks.
# MAGIC
# MAGIC The naming pattern is:
# MAGIC
# MAGIC catalog.schema.table
# MAGIC
# MAGIC Example:
# MAGIC iot_berkeley_lab.bronze.sensor_readings_raw

# COMMAND ----------

# DBTITLE 1,Define Unity Catalog table names
# Bronze tables
bronze_sensor_readings_table = f"{catalog_name}.{bronze_schema}.sensor_readings_raw"
bronze_sensor_metadata_table = f"{catalog_name}.{bronze_schema}.sensor_metadata_raw"

# Silver tables
silver_sensor_readings_clean_table = f"{catalog_name}.{silver_schema}.sensor_readings_clean"
silver_sensor_metadata_clean_table = f"{catalog_name}.{silver_schema}.sensor_metadata_clean"
silver_sensor_readings_enriched_table = f"{catalog_name}.{silver_schema}.sensor_readings_enriched"

# Gold dimension tables
gold_dim_mote_table = f"{catalog_name}.{gold_schema}.dim_mote"
gold_dim_date_table = f"{catalog_name}.{gold_schema}.dim_date"
gold_dim_time_table = f"{catalog_name}.{gold_schema}.dim_time"

# Gold fact tables
gold_fact_sensor_reading_table = f"{catalog_name}.{gold_schema}.fact_sensor_reading"
gold_fact_sensor_hourly_table = f"{catalog_name}.{gold_schema}.fact_sensor_hourly"
gold_fact_sensor_daily_table = f"{catalog_name}.{gold_schema}.fact_sensor_daily"
gold_fact_sensor_health_table = f"{catalog_name}.{gold_schema}.fact_sensor_health"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Create ingestion date parameter
# MAGIC
# MAGIC This step creates an optional Databricks widget called ingest_date.
# MAGIC
# MAGIC If the user provides an ingestion date, the notebooks will process that specific landing folder.
# MAGIC
# MAGIC If the widget is left empty, the notebook will automatically detect the latest available ingest_date folder from the landing layer.
# MAGIC
# MAGIC Expected folder pattern:
# MAGIC ingest_date=YYYY-MM-DD

# COMMAND ----------

# DBTITLE 1,Create ingestion date parameter
# Create an optional widget for manual ingestion date selection
dbutils.widgets.text("ingest_date", "", "Ingestion date yyyy-MM-dd")

# Read the widget value
input_ingest_date = dbutils.widgets.get("ingest_date").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Define helper function to detect the latest ingestion date
# MAGIC
# MAGIC This helper function lists the landing folder and extracts the latest available ingest_date value.
# MAGIC
# MAGIC This is useful because the source is static and the pipeline is manually triggered, so the processing date may not always match the current system date.

# COMMAND ----------

# DBTITLE 1,Define helper function to detect the latest ingestion date
import re
from datetime import datetime

def get_latest_ingest_date(root_path: str) -> str:
    """
    Detects the latest ingest_date folder inside a landing root path.

    Expected folder format:
    ingest_date=YYYY-MM-DD

    Parameters:
    root_path: ADLS path where ingest_date folders are stored.

    Returns:
    Latest ingestion date as a string in yyyy-MM-dd format.
    """

    folders = dbutils.fs.ls(root_path)

    ingest_dates = []

    for folder in folders:
        folder_name = folder.name.rstrip("/")

        match = re.match(r"ingest_date=(\d{4}-\d{2}-\d{2})", folder_name)

        if match:
            ingest_dates.append(match.group(1))

    if not ingest_dates:
        raise ValueError(f"No ingest_date folders found in path: {root_path}")

    return max(ingest_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Resolve the ingestion date to process
# MAGIC
# MAGIC This step decides which ingestion date will be processed.
# MAGIC
# MAGIC If the ingest_date widget has a value, that value is used.
# MAGIC If it is empty, the notebook automatically detects the latest ingest_date folder from the sensor readings landing path.

# COMMAND ----------

# DBTITLE 1,Resolve the ingestion date to process
if input_ingest_date:
    ingest_date = input_ingest_date
else:
    ingest_date = get_latest_ingest_date(landing_sensor_readings_root)

print(f"Ingestion date selected: {ingest_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Define source file paths for the selected ingestion date
# MAGIC
# MAGIC This step builds the exact file paths for the two source files.
# MAGIC
# MAGIC The source files do not contain headers, so downstream notebooks must manually assign column names.

# COMMAND ----------

# DBTITLE 1,Define source file paths for the selected ingestion date
# Landing folders for the selected ingestion date
landing_sensor_readings_ingest_path = f"{landing_sensor_readings_root}ingest_date={ingest_date}/"
landing_sensor_metadata_ingest_path = f"{landing_sensor_metadata_root}ingest_date={ingest_date}/"

# Expected source files
sensor_readings_file_path = f"{landing_sensor_readings_ingest_path}data.txt.gz"
sensor_metadata_file_path = f"{landing_sensor_metadata_ingest_path}mote_locs.txt"

print(sensor_readings_file_path)
print(sensor_metadata_file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate that expected landing files exist
# MAGIC
# MAGIC This step validates that the expected source files are available in the selected landing folders.
# MAGIC
# MAGIC This does not replace Azure Data Factory validation. It adds a second validation inside Databricks before processing.

# COMMAND ----------

# DBTITLE 1,Validate that expected landing files exist
def path_exists(path: str) -> bool:
    """
    Checks whether a file or folder exists in ADLS.

    Parameters:
    path: ADLS path to validate.

    Returns:
    True if the path exists, otherwise False.
    """

    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


sensor_readings_exists = path_exists(sensor_readings_file_path)
sensor_metadata_exists = path_exists(sensor_metadata_file_path)

if not sensor_readings_exists:
    raise FileNotFoundError(f"Sensor readings file not found: {sensor_readings_file_path}")

if not sensor_metadata_exists:
    raise FileNotFoundError(f"Sensor metadata file not found: {sensor_metadata_file_path}")

print("Landing files validated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Set active Unity Catalog catalog
# MAGIC
# MAGIC This step sets the active Unity Catalog catalog for the Spark session.
# MAGIC
# MAGIC Later notebooks will write tables using fully qualified names, but setting the active catalog helps keep the session aligned with the project.

# COMMAND ----------

# DBTITLE 1,Set active Unity Catalog catalog
spark.sql(f"USE CATALOG {catalog_name}")

print(f"Active catalog set to: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Display final configuration summary
# MAGIC
# MAGIC This step displays the resolved configuration so the user can quickly verify the selected ingestion date, paths and table names before running downstream notebooks.

# COMMAND ----------

# DBTITLE 1,Display final configuration summary
config_summary = [
    ("project_name", project_name),
    ("catalog_name", catalog_name),
    ("bronze_schema", bronze_schema),
    ("silver_schema", silver_schema),
    ("gold_schema", gold_schema),
    ("ingest_date", ingest_date),
    ("base_path", base_path),
    ("sensor_readings_file_path", sensor_readings_file_path),
    ("sensor_metadata_file_path", sensor_metadata_file_path),
    ("bronze_sensor_readings_table", bronze_sensor_readings_table),
    ("bronze_sensor_metadata_table", bronze_sensor_metadata_table),
    ("silver_sensor_readings_clean_table", silver_sensor_readings_clean_table),
    ("silver_sensor_metadata_clean_table", silver_sensor_metadata_clean_table),
    ("silver_sensor_readings_enriched_table", silver_sensor_readings_enriched_table),
    ("gold_fact_sensor_reading_table", gold_fact_sensor_reading_table),
    ("gold_fact_sensor_hourly_table", gold_fact_sensor_hourly_table),
    ("gold_fact_sensor_daily_table", gold_fact_sensor_daily_table),
    ("gold_fact_sensor_health_table", gold_fact_sensor_health_table)
]

config_df = spark.createDataFrame(config_summary, ["config_key", "config_value"])

display(config_df)