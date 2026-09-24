# Snowflake

Database used in the project:

```text
SHOPSPHERE_DW
```

Schemas:

```text
STAGING
ANALYTICS
```

## External serving path

The live project uses ADLS Gold Parquet, exposed through:

```text
STG_GOLD_FACT_SALES
STG_GOLD_FACT_SALES_INCREMENTAL
```

with storage integration:

```text
INT_SHOPSPHERE_ADLS
```

## Serving logic

```text
ADLS Parquet
  ↓ COPY INTO
STAGING.STG_FACT_SALES_INCREMENTAL
  ↓ MERGE
ANALYTICS.FACT_SALES
```

The DDL files are sanitized/reconstructed from the live object names and observed schemas. Run `SHOW CREATE` in the live environment if you need an exact account-specific export.
