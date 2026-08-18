# Azure Synapse Analytics Serverless SQL Serving Layer

## Overview

Azure Synapse Analytics Serverless SQL was implemented as the serving layer for the **Az-IOT-Berkeley — IoT Berkeley Lab Analytics Platform** project.

The purpose of this phase was to expose the curated Gold layer stored in Azure Data Lake Storage Gen2 through SQL views, without copying or materializing the data into a dedicated SQL warehouse. Synapse Serverless SQL provides a lightweight SQL access layer over the Delta Lake folders created in the previous processing phase.

This phase focuses only on data serving and SQL validation. No additional data transformations, persistent log tables, external tables, CETAS outputs, or dedicated SQL pools were created.

## Role in the Architecture

Synapse Serverless SQL sits between the Gold layer in ADLS Gen2 and future analytical consumption.

The implemented flow is:

```text
ADLS Gen2 Gold Delta folders
        ↓
Azure Synapse Serverless SQL
        ↓
SQL views
        ↓
Analytical consumption
```

The Gold data remains physically stored in ADLS Gen2. Synapse stores only SQL metadata such as the database, external data source, credential, and views.

## Serverless SQL Database

A Serverless SQL database was defined for the project:

```sql
iot_berkeley_lab_sql
```

This database is used to organize the SQL serving objects created for the IoT Berkeley Lab analytics model.

The database was configured with UTF-8 collation:

```sql
Latin1_General_100_BIN2_UTF8
```

## External Access Configuration

Synapse Serverless SQL was configured to access the project Data Lake through the workspace Managed Identity.

The following database scoped credential was defined:

```sql
synapse_managed_identity
```

The following external data source was defined:

```sql
iot_berkeley_lake
```

It points to the project container in ADLS Gen2:

```text
https://saiotberkeley.dfs.core.windows.net/iot-berkeley-lab
```

This allows the views to reference Gold folders using relative paths such as:

```text
gold/dim_mote/
gold/fact_sensor_daily/
```

## Gold Views

The following SQL views were created under the default `dbo` schema.

### Dimension Views

| View | Source Gold Folder | Purpose |
| --- | --- | --- |
| `dbo.vw_dim_mote` | `gold/dim_mote/` | Exposes sensor metadata, including mote position and metadata availability. |
| `dbo.vw_dim_date` | `gold/dim_date/` | Exposes calendar attributes for date-based analysis. |
| `dbo.vw_dim_time` | `gold/dim_time/` | Exposes time attributes for hourly and time-of-day analysis. |

### Fact Views

| View | Source Gold Folder | Purpose |
| --- | --- | --- |
| `dbo.vw_fact_sensor_reading` | `gold/fact_sensor_reading/` | Exposes one valid row per sensor reading. |
| `dbo.vw_fact_sensor_hourly` | `gold/fact_sensor_hourly/` | Exposes hourly aggregated sensor metrics. |
| `dbo.vw_fact_sensor_daily` | `gold/fact_sensor_daily/` | Exposes daily aggregated sensor metrics. |
| `dbo.vw_fact_sensor_health` | `gold/fact_sensor_health/` | Exposes daily sensor health metrics by mote. |

Each view reads Delta files directly using:

```sql
OPENROWSET(
    BULK '<gold-folder-path>',
    DATA_SOURCE = 'iot_berkeley_lake',
    FORMAT = 'DELTA'
)
```

## Implemented SQL Objects

| Object | Type | Status |
| --- | --- | --- |
| `iot_berkeley_lab_sql` | Serverless SQL database | Created |
| `dbo` | Default schema | Used |
| `synapse_managed_identity` | Database scoped credential | Created |
| `iot_berkeley_lake` | External data source | Created |
| `dbo.vw_dim_mote` | View | Created and validated |
| `dbo.vw_dim_date` | View | Created and validated |
| `dbo.vw_dim_time` | View | Created and validated |
| `dbo.vw_fact_sensor_reading` | View | Created and validated |
| `dbo.vw_fact_sensor_hourly` | View | Created and validated |
| `dbo.vw_fact_sensor_daily` | View | Created and validated |
| `dbo.vw_fact_sensor_health` | View | Created and validated |



## Validation Queries

The Synapse serving layer was validated using SQL queries for:

- View existence.
- Row counts by view.
- Column metadata and data types.
- Null checks on key columns.
- Duplicate checks according to each table grain.
- Logical relationship checks between fact and dimension views.
- Basic analytical queries.

The validation queries included:

- Daily average temperature.
- Active sensors by day.
- Minimum voltage by sensor.
- Low-voltage readings by day.
- Hourly sensor activity analysis.
- Sensors without available metadata.

## Design Decisions

The serving layer was intentionally kept simple.

Key design decisions:

- Use Serverless SQL instead of Dedicated SQL Pool to keep costs low.
- Query Gold Delta folders directly from ADLS Gen2.
- Expose data through SQL views instead of duplicating it.
- Avoid external tables because views were sufficient for this project scope.
- Avoid CETAS because no additional materialized output was required.
- Avoid persistent logs or extra metadata tables without a clear analytical purpose.
- Keep the model easy to understand, query, and document for portfolio purposes.

## Final Status

The Synapse Serverless SQL phase was completed as a lightweight serving layer over the Gold data stored in ADLS Gen2.

The final output of this phase is a set of SQL views that expose curated IoT sensor dimensions and facts for analytical consumption.