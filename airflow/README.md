# Apache Airflow

Airflow runs under WSL/Linux in the working development setup.

## DAG

`dags/shopsphere_pipeline.py` orchestrates:

```text
start_pipeline
  → trigger_adf_incremental
  → trigger_databricks_incremental
  → load_snowflake_incremental
  → update_pipeline_control
  → pipeline_complete
```

## Airflow connections

```text
azure_data_factory_default
databricks_default
snowflake_default
```

Never commit connection passwords or generated auth files. Use Airflow Connections/secret backends.
