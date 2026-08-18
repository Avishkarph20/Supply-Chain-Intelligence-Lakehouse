# Supply Chain Intelligence Lakehouse

An end-to-end Azure-based data engineering project that transforms raw supply chain data into actionable business insights using a Lakehouse architecture.

## Project Overview

The project implements a cloud-based data pipeline for processing supply chain data such as inventory, suppliers, warehouses, logistics and orders.

The pipeline follows a Medallion Architecture to ingest, clean, transform and prepare data for analytics and visualization.

## Architecture

Raw Data
   ↓
Azure Data Factory
   ↓
Azure Data Lake Storage Gen2
   ↓
Azure Databricks
   ↓
PySpark / Spark SQL
   ↓
Delta Lake
   ↓
Power BI
   ↓
Business Insights

## Technologies Used

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Spark SQL
- Delta Lake
- Power BI

## Medallion Architecture

### Bronze Layer
Stores the raw ingested supply chain data with minimal transformation.

### Silver Layer
Cleans and validates the raw data and performs required transformations.

### Gold Layer
Contains processed and business-ready datasets used for analytics and reporting.

## Data Pipeline

1. Raw supply chain data is ingested using Azure Data Factory.
2. Data is stored in Azure Data Lake Storage Gen2.
3. Azure Databricks processes the data using PySpark and Spark SQL.
4. Data quality validation and transformations are performed.
5. Processed data is stored using Delta Lake.
6. Business-ready data is consumed by Power BI.
7. Power BI dashboards provide supply chain insights.

## Business Insights

The project focuses on analyzing:

- Inventory levels
- Supplier performance
- Warehouse operations
- Logistics KPIs
- Order fulfillment trends

## Project Structure

```text
Supply-Chain-Intelligence-Lakehouse/
│
├── data/
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/
├── powerbi/
├── architecture/
└── README.md
