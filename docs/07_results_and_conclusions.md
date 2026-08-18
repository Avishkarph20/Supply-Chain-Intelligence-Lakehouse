# Results and Conclusions

## Completed Results

The project successfully implemented the core Azure data engineering workflow up to the SQL serving layer.

Completed phases:

- Public IoT source files were ingested into ADLS Gen2 landing using Azure Data Factory.
- Raw files were preserved without modification in the landing zone.
- Databricks processed the files through Bronze, Silver, and Gold layers.
- Invalid records were separated into rejected zones.
- Gold Delta tables were created for analytical consumption.
- Synapse Serverless SQL exposed the Gold layer through SQL views.

## Main Technical Outcomes

- Clear separation of responsibilities across Azure services.
- Reusable ADF linked services and parameterized binary datasets.
- Lakehouse processing flow with Delta Lake.
- Unity Catalog organization using catalog and schemas.
- Dimensional Gold model with facts and dimensions.
- Lightweight SQL serving layer without copying or materializing Gold data.

## Important Design Choices

- ADF was not used for heavy transformations.
- Daily ingestion trigger was intentionally excluded because the source data is static.
- Persistent operational logs were avoided when they did not add clear value.
- Gold tables exclude technical lineage columns that do not provide direct analytical value.
- Synapse views were preferred over external tables and CETAS outputs for this project scope.