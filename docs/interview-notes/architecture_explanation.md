# Interview Architecture Explanation

## 60-second answer

ShopSphere is an end-to-end incremental e-commerce data platform. MySQL is the source. Azure Data Factory uses metadata and watermarks to extract only new/changed records into ADLS Gen2. Databricks Auto Loader loads those files into Delta Bronze, while Silver handles data quality, deduplication, SCD Type 2 for customers, and referential integrity. Gold creates an order-level analytical fact and exports Parquet to ADLS. Snowflake loads the Gold data with COPY INTO and MERGE. Apache Airflow orchestrates ADF, the Databricks Job, Snowflake loading, and the final watermark update.

## Why ADF and Airflow together?

ADF is responsible for source-to-lake ingestion. Airflow is the outer orchestrator that coordinates multiple platforms and waits for downstream completion before advancing control state.

## Why Delta Lake?

Delta provides ACID transactions, schema enforcement, and a natural foundation for incremental merge/update patterns in the lakehouse.

## Why Snowflake after Databricks?

Databricks is used as the transformation/lakehouse engine, while Snowflake is the analytical serving layer. Keeping those responsibilities distinct reduces duplicate transformation logic.
