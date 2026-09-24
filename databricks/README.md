# Databricks / Delta Lake

Catalog:

```text
SHOPSPHERE
```

Schemas:

```text
bronze
silver
gold
error
```

The `.py` files are notebook-source exports / reference implementations corresponding to the working ShopSphere pipeline. They intentionally use placeholders only where environment-specific paths would otherwise leak infrastructure details.

## One-time vs recurring

- Initial full-load notebooks contain an explicit one-time overwrite block. After the initial population succeeds, those cells must be skipped.
- Auto Loader notebooks use `availableNow=True` and append into Bronze.
- Silver notebooks maintain current/history state without recreating the initial tables.
- Gold exports incremental Parquet for Snowflake serving.
