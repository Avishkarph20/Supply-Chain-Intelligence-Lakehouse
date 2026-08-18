/* ============================================================
   DATABASE CREATION
   ============================================================ */

CREATE DATABASE iot_berkeley_lab_sql;
GO

USE iot_berkeley_lab_sql;
GO

ALTER DATABASE iot_berkeley_lab_sql
COLLATE Latin1_General_100_BIN2_UTF8;
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '7895123Dd@7895123';
GO

CREATE DATABASE SCOPED CREDENTIAL synapse_managed_identity
WITH IDENTITY = 'Managed Identity';
GO

CREATE EXTERNAL DATA SOURCE iot_berkeley_lake
WITH (
    LOCATION = 'https://saiotberkeley.dfs.core.windows.net/iot-berkeley-lab',
    CREDENTIAL = synapse_managed_identity
);
GO

/* ============================================================
   DELTA TABLES VERIFICATIONS
   ============================================================ */

USE iot_berkeley_lab_sql;
GO

SELECT TOP 10 *
FROM OPENROWSET(
    BULK 'gold/dim_mote/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
) AS rows;

SELECT TOP 10 *
FROM OPENROWSET(
    BULK 'gold/fact_sensor_daily/',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
) AS rows;
