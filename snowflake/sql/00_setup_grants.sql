-- Service-principal permissions used by the automated Databricks/Airflow pipeline.
-- Replace APP_ID with your own service principal Application ID.
-- The live project used the application ID rather than display name in Unity Catalog SQL.

-- Databricks grants are executed in Databricks SQL, not Snowflake.
-- This file is kept as documentation only.

-- Example Databricks SQL:
-- GRANT USE CATALOG ON CATALOG SHOPSPHERE TO `<APP_ID>`;
-- GRANT USE SCHEMA ON SCHEMA SHOPSPHERE.bronze TO `<APP_ID>`;
-- GRANT SELECT, MODIFY ON SCHEMA SHOPSPHERE.bronze TO `<APP_ID>`;
