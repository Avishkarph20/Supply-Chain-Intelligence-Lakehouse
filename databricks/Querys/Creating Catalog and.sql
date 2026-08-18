-- CREATING CATALOG --

CREATE CATALOG IF NOT EXISTS iot_berkeley_lab
MANAGED LOCATION 'abfss://iot-berkeley-lab@saiotberkeley.dfs.core.windows.net/_uc_managed/iot_berkeley_lab/'
COMMENT 'Unity Catalog catalog for the IoT Berkeley Lab Analytics Platform project';

USE CATALOG iot_berkeley_lab;

-- CREATING BRONZE SQUEMA IN CATALOG --
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze layer: raw data preserved from landing with technical metadata';

-- CREATING SILVER SQUEMA IN CATALOG --
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver layer: cleaned, typed, validated and enriched IoT sensor data';

-- CREATING GOLD SQUEMA IN CATALOG --
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold layer: dimensional and analytical tables ready for Synapse and Power BI';