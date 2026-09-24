# Live Export Guide

The repository intentionally contains sanitized definitions. To replace them with exact live exports:

## ADF

From Azure Data Factory Studio:

1. Open the live pipeline.
2. Export pipeline JSON / ARM template according to your deployment process.
3. Save pipeline JSON under `adf/pipelines/`.
4. Export/import datasets and linked-service definitions under their respective folders.
5. Remove credentials/secrets before committing.

## Databricks

Export each notebook as source (`.py`) from the workspace and place it in the matching `databricks/bronze`, `silver`, `gold`, or `control` directory.

For Job configuration, store a sanitized job definition under `docs/` or `databricks/` rather than copying tokens.

## Snowflake

Use `GET_DDL` or `SHOW CREATE`/object metadata commands to capture exact definitions. Keep storage-integration credentials and passwords out of Git.

Examples:

```sql
SELECT GET_DDL('TABLE', 'SHOPSPHERE_DW.ANALYTICS.FACT_SALES');
SELECT GET_DDL('TABLE', 'SHOPSPHERE_DW.STAGING.STG_FACT_SALES_INCREMENTAL');
```

Also capture stages, file formats, schemas and views where appropriate.
