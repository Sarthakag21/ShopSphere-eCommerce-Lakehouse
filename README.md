# ShopSphere — End-to-End E-Commerce Data Engineering Lakehouse

![Azure](https://img.shields.io/badge/Azure-Data%20Platform-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Data%20Lake-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-E01E5A?style=flat-square&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Storage-0B6E99?style=flat-square)
![Snowflake](https://img.shields.io/badge/Snowflake-Warehouse-29B5E8?style=flat-square&logo=snowflake&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-OLTP-4479A1?style=flat-square&logo=mysql&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Transformations-FDEE21?style=flat-square&logo=apachespark&logoColor=black)

> Production-style e-commerce data platform demonstrating metadata-driven incremental ingestion, Delta Lake Medallion Architecture, data quality, SCD Type 2, Snowflake serving, and Airflow orchestration.

---

## 1. Project at a Glance

**ShopSphere** ingests transactional e-commerce data from MySQL, moves only new/changed rows through Azure Data Factory and ADLS Gen2, processes those files with Databricks/Delta Lake, publishes a Gold fact model, and serves the result in Snowflake.

Apache Airflow is the outer orchestrator and controls the final sequence:

```text
MySQL
  ↓
Azure Data Factory (metadata-driven incremental extraction)
  ↓
ADLS Gen2 / landing
  ↓
Databricks Auto Loader
  ↓
Bronze (raw Delta)
  ↓
Silver (DQ + dedup + SCD2 + RI)
  ↓
Gold (business-ready fact)
  ↓
ADLS Gen2 / Gold Parquet
  ↓
Snowflake STAGING (COPY INTO)
  ↓
Snowflake ANALYTICS.FACT_SALES (MERGE)
  ↓
Databricks pipeline_control watermark update
```

Airflow DAG:

```text
start_pipeline
      ↓
trigger_adf_incremental
      ↓
trigger_databricks_incremental
      ↓
load_snowflake_incremental
      ↓
update_pipeline_control
      ↓
pipeline_complete
```

---

## 2. Why this Architecture?

The project intentionally separates responsibilities:

| Component | Responsibility |
|---|---|
| MySQL | OLTP / transactional source |
| ADF | Metadata-driven incremental extraction |
| ADLS Gen2 | Durable cloud landing and Gold exchange layer |
| Databricks | Distributed ETL, Delta Lake, DQ, SCD2, RI, Gold modeling |
| Snowflake | Analytical serving / warehouse |
| Airflow | Cross-platform orchestration and dependency management |
| Pipeline Control | Last-successful-watermark state |

This avoids turning Snowflake into a second transformation platform and keeps Databricks responsible for lakehouse transformations while Snowflake serves curated analytics.

---

## 3. Source Data Model

Database:

```text
SHOPSPHERE_SOURCE
```

Tables:

| Table | Main key | Incremental column | Role |
|---|---|---|---|
| `customers` | `customer_id` | `updated_at` | Customer master |
| `products` | `product_id` | `updated_at` | Product master |
| `orders` | `order_id` | `updated_at` | Order fact source |
| `payments` | `payment_id` | `payment_ts` | Payment events |
| `shipments` | `shipment_event_id` | `event_ts` | Shipment events |

`payments` and `shipments` are treated as event-like sources. Their incremental pattern uses event timestamps rather than a mutable `updated_at` column.

---

## 4. Azure Data Factory — Incremental Ingestion

### Metadata-driven pattern

ADF reads source metadata and loops through active tables. The core metadata fields are:

```text
source_table
watermark_column
last_watermark
target_path
is_active
```

Incremental extraction follows this pattern:

```sql
SELECT *
FROM <source_table>
WHERE <watermark_column> > '<last_watermark>'
  AND <watermark_column> <= NOW();
```

ADF stores the copied files in table-specific ADLS paths:

```text
landing/customers/incremental/
landing/products/incremental/
landing/orders/incremental/
landing/payments/incremental/
landing/shipments/incremental/
```

The watermark is not advanced until the copy succeeds.

See the sanitized ADF definitions under [`adf/`](./adf/).

---

## 5. ADLS Gen2 Layout

```text
shopsphere/
├── landing/
│   ├── customers/
│   │   ├── initial/
│   │   └── incremental/
│   ├── products/
│   ├── orders/
│   ├── payments/
│   └── shipments/
│
├── bronze/                 # Databricks Delta tables / managed data
├── silver/
├── gold/
│   └── serving/
│       └── fact_sales_incremental/
│
└── checkpoints/            # Auto Loader / streaming checkpoints
```

The live project uses ADLS paths such as:

```text
abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/
```

---

## 6. Databricks Lakehouse

Unity Catalog catalog:

```text
SHOPSPHERE
```

Schemas:

```text
SHOPSPHERE.bronze
SHOPSPHERE.silver
SHOPSPHERE.gold
SHOPSPHERE.error
```

### Bronze

- Auto Loader for incremental file ingestion
- Delta format
- Append-oriented processing
- Explicit schema where source ambiguity existed
- `_ingested_at` audit timestamp
- `_source_file` lineage metadata

### Silver

Customers:

- Data quality
- SCD Type 2 history
- Current-row tracking

Products:

- Data quality
- Current-state deduplication

Orders:

- Null/business-rule validation
- Deduplication by latest `updated_at`
- Customer/product referential checks
- Error quarantine

Payments:

- Data quality
- Deduplication by payment event
- Order referential checks
- Error quarantine

Shipments:

- Incremental event ingestion
- Deduplication / current shipment state
- Order referential checks

### Gold

The business-ready `fact_sales` model combines order, customer, product, payment and shipment information at order level.

Representative columns include:

```text
order_id
order_date
order_status
customer_id
customer_name
city
state
country
product_id
product_name
category
unit_price
quantity
gross_revenue
total_paid
latest_payment_ts
shipment_status
shipment_carrier
latest_shipment_ts
updated_at
_ingested_at
```

---

## 7. Data Quality & Error Handling

The project intentionally demonstrates failure isolation.

### Customer DQ examples

```text
customer_id is not null
customer_name is not null
email is not null
email format is valid
country is not null
signup_date is not null
updated_at is not null
signup_date is not in the future
```

### Product DQ examples

```text
product_id is not null
product_name is not null
category is not null
price is not null
price >= 0
currency is not null
launch_date is not null
updated_at is not null
```

### Orders / Payments / Shipments

Invalid records are quarantined in the `SHOPSPHERE.error` schema rather than silently discarded.

---

## 8. SCD Type 2 — Customer Dimension

`SHOPSPHERE.silver.dim_customers` keeps historical customer versions with:

```text
effective_from
effective_to
is_current
```

When a customer attribute changes:

```text
old row → effective_to = change timestamp, is_current = false
new row → effective_from = change timestamp, is_current = true
```

This preserves historical reporting while still providing a current dimension row.

---

## 9. Snowflake Serving Layer

Database:

```text
SHOPSPHERE_DW
```

Schemas:

```text
STAGING
ANALYTICS
```

External stages:

```text
STG_GOLD_FACT_SALES
STG_GOLD_FACT_SALES_INCREMENTAL
```

File format:

```text
FF_PARQUET
```

Tables:

```text
STAGING.STG_FACT_SALES_INCREMENTAL
ANALYTICS.FACT_SALES
```

### Serving pattern

```sql
COPY INTO SHOPSPHERE_DW.STAGING.STG_FACT_SALES_INCREMENTAL
FROM @SHOPSPHERE_DW.STAGING.STG_GOLD_FACT_SALES_INCREMENTAL
...
```

followed by:

```sql
MERGE INTO SHOPSPHERE_DW.ANALYTICS.FACT_SALES ...
```

The staging load relies on Snowflake's file-load metadata so the same staged file is not repeatedly loaded by normal `COPY INTO` execution.

---

## 10. Airflow Orchestration

Airflow 3.3.x runs under WSL/Linux in the local development environment.

The DAG uses:

- `AzureDataFactoryRunPipelineOperator`
- `DatabricksRunNowOperator`
- `SQLExecuteQueryOperator`
- `DatabricksSQLStatementsOperator`

Connections used by the DAG:

```text
azure_data_factory_default
databricks_default
snowflake_default
```

The Databricks Job is configured to **Run as** the service principal `shopsphere-airflow`.

---

## 11. Retry-Safe Watermark Design

The most important orchestration rule is:

```text
ADF success
    ↓
Databricks success
    ↓
Snowflake COPY + MERGE success
    ↓
Update pipeline_control
```

If Snowflake fails:

```text
Snowflake ❌
    ↓
Watermark NOT advanced
    ↓
Retry can safely reprocess the same source window
```

The control table is:

```text
SHOPSPHERE.gold.pipeline_control
```

with:

```text
source_name
watermark_column
last_watermark
status
```

---

## 12. Validation Evidence

The repository includes screenshots under [`docs/screenshots/`](./docs/screenshots/) showing:

- Databricks incremental job graph
- Gold incremental output
- Snowflake staging verification
- Snowflake `COPY INTO` + `MERGE`
- Final Snowflake `FACT_SALES` verification
- Final pipeline-control/watermark state

Observed final validation from the project run included:

```text
Snowflake FACT_SALES rows: 50002
```

and a tested order with:

```text
order_status      = SHIPPED
total_paid        = 224944.59
shipment_status   = OUT_FOR_DELIVERY
shipment_carrier  = Delhivery
```

All five pipeline-control rows showed `SUCCESS` in the final validation.

---

## 13. Technology Stack

```text
Source        : MySQL
Ingestion     : Azure Data Factory
Storage       : Azure Data Lake Storage Gen2
Processing    : Azure Databricks + PySpark
Storage       : Delta Lake
Governance    : Unity Catalog
Orchestration : Apache Airflow
Warehouse     : Snowflake
Formats       : CSV / JSONL / Parquet / Delta
Languages     : SQL / Python / PySpark
BI            : Snowflake-ready; Power BI was intentionally not included in the final project scope
```

---

## 14. Project Structure

```text
ShopSphere-ECommerce-Lakehouse/
│
├── README.md
├── LICENSE
├── .gitignore
├── requirements.txt
│
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   ├── linked-services/
│   └── README.md
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── control/
│   └── README.md
│
├── airflow/
│   ├── dags/
│   ├── requirements.txt
│   └── README.md
│
├── snowflake/
│   ├── databases/
│   ├── schemas/
│   ├── stages/
│   ├── file_formats/
│   ├── tables/
│   ├── sql/
│   └── README.md
│
├── sql/
│   ├── source_schema.sql
│   ├── test_data.sql
│   └── validation.sql
│
├── docs/
│   ├── architecture/
│   ├── screenshots/
│   └── interview-notes/
│
└── data/
    └── sample-data/
```

---

## 15. Local Setup — High Level

### MySQL

```sql
SOURCE sql/source_schema.sql;
```

Load or generate training data, then configure ADF metadata.

### Azure

Create or configure:

```text
Resource Group
ADLS Gen2
Azure Data Factory
Azure Databricks
Access Connector / Managed Identity
Unity Catalog
```

### Databricks

Import the scripts under `databricks/` as notebooks or `.py` source files and update cloud-specific paths for your environment.

### Snowflake

Run the object DDL in:

```text
snowflake/databases/
snowflake/schemas/
snowflake/stages/
snowflake/file_formats/
snowflake/tables/
```

then execute the serving SQL under `snowflake/sql/`.

### Airflow

Install Airflow and provider packages from the project requirements, create the three connections, place the DAG under `$AIRFLOW_HOME/dags/`, and start Airflow using a supported Linux/WSL environment.

---

## 16. Security Notes

Never commit:

```text
Snowflake passwords
airflow auth files
Azure client secrets
Databricks tokens
private keys
connection strings with embedded secrets
production data
```

The repository contains sanitized configuration references and examples. Secrets should be supplied through environment variables, Airflow Connections, Key Vault/secret managers, or workspace secret scopes.

---

## 17. Cost Optimization Decisions

The local project used serverless Databricks execution where possible and avoided long-lived development clusters.

Practical cost controls used during development:

- Use `availableNow=True` for Auto Loader tests
- Avoid rerunning initial overwrite cells
- Use small serverless compute for notebook tests
- Keep unused SQL warehouses stopped
- Run verification queries only when needed
- Do not recreate infrastructure during every experiment
- Use retries at orchestration level instead of repeatedly launching manual jobs

---

## 18. Interview Talking Points

A strong project explanation can be summarized as:

> "I built an end-to-end incremental e-commerce data platform. MySQL is the source, ADF is metadata-driven for incremental extraction, and ADLS Gen2 is the landing layer. Databricks Auto Loader ingests files into Bronze Delta tables. Silver handles data quality, deduplication, SCD Type 2 for customers, and referential-integrity checks. Gold builds the order-level business fact and exports Parquet to ADLS. Snowflake loads the Gold files with COPY INTO and merges them into FACT_SALES. Airflow orchestrates ADF, the Databricks Job, Snowflake, and finally advances the pipeline watermark only after the warehouse load succeeds." 

See [`docs/interview-notes/`](./docs/interview-notes/) for scenario-based questions and concise answers.

---

## 19. Known Repository Limitations

Some cloud objects in the live environment were configured interactively. The checked-in ADF JSON and some Snowflake DDL files are **sanitized/reconstructed project definitions**, not raw exports containing account-specific secrets or workspace metadata. The repository README and file-level notes identify these cases.

For exact production recreation, export the current ADF/Databricks definitions from your live workspace and replace the sanitized files while keeping credentials out of Git.

---

## 20. Future Enhancements

- Event Hubs → Databricks Structured Streaming branch
- CDC with SQL Server/MySQL binlog or Debezium
- Automated data-quality metrics table
- Azure Monitor / Log Analytics integration
- CI/CD with GitHub Actions
- Terraform/Bicep infrastructure-as-code
- Snowflake Streams + Tasks alternative serving pattern
- Schema evolution tests
- Automated unit/integration tests for PySpark transformations
- Data lineage documentation

---

## License

MIT — see [`LICENSE`](./LICENSE).
