# ShopSphere Architecture

See the PNG architecture diagrams in this directory for a visual overview.

## Logical layers

1. **MySQL** — operational source.
2. **ADF** — incremental extraction and watermark-controlled file landing.
3. **ADLS Gen2** — raw landing and Gold exchange/storage.
4. **Databricks** — Bronze/Silver/Gold transformations.
5. **Snowflake** — analytical serving.
6. **Airflow** — cross-platform orchestration.
7. **Pipeline control** — successful watermark state.

## Orchestration dependency

```text
ADF → Databricks → Snowflake → watermark update
```

The architecture deliberately avoids a Power BI dependency in the implementation scope.
