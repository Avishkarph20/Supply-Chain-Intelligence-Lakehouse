# Project Overview

## Project Name

**Az-IOT-Berkeley — IoT Berkeley Lab Analytics Platform**

## Objective

This project implements an Azure-based data engineering platform for processing and serving IoT sensor data from the Intel Berkeley Lab dataset.

The main goal is to demonstrate an end-to-end data engineering workflow that moves raw public source files into a cloud data lake, processes them through a lakehouse architecture, builds curated analytical Delta tables, and exposes the final Gold model through SQL views.

## Implemented Scope

The current project scope includes:

- Raw file ingestion with Azure Data Factory.
- Raw, processed, curated, and rejected storage zones in ADLS Gen2.
- Bronze, Silver, and Gold processing with Azure Databricks.
- Delta Lake table creation.
- Unity Catalog organization.
- Synapse Serverless SQL views over the Gold Delta folders.

## Source Files

| Source File | Description |
| --- | --- |
| `data.txt.gz` | Main IoT sensor readings file. |
| `mote_locs.txt` | Sensor metadata file with mote location data. |

The files do not contain headers. Schema parsing is therefore handled in the Databricks Silver layer, not in Azure Data Factory.

## Main Design Principle

Each platform component has a clear responsibility:

| Component | Responsibility |
| --- | --- |
| Azure Data Factory | Ingest raw files and validate landing file existence and size. |
| ADLS Gen2 | Store landing, Bronze, Silver, Gold, and rejected data. |
| Azure Databricks | Parse, validate, enrich, and model the data. |
| Delta Lake | Store reliable lakehouse tables. |
| Unity Catalog | Organize governed tables by catalog and schema. |
| Synapse Serverless SQL | Expose the Gold model through SQL views. |
