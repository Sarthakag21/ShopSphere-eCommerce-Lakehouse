-- Control state for successful incremental processing.
-- Run as a Databricks SQL admin/owner once to create the table.

CREATE TABLE IF NOT EXISTS SHOPSPHERE.gold.pipeline_control (
    source_name      STRING,
    watermark_column STRING,
    last_watermark   TIMESTAMP,
    status           STRING
)
USING DELTA;

MERGE INTO SHOPSPHERE.gold.pipeline_control tgt
USING (
    SELECT * FROM VALUES
      ('customers', 'effective_from', TIMESTAMP('1900-01-01'), 'SUCCESS'),
      ('products',  'updated_at',    TIMESTAMP('1900-01-01'), 'SUCCESS'),
      ('orders',    'updated_at',    TIMESTAMP('1900-01-01'), 'SUCCESS'),
      ('payments',  'payment_ts',    TIMESTAMP('1900-01-01'), 'SUCCESS'),
      ('shipments', 'event_ts',      TIMESTAMP('1900-01-01'), 'SUCCESS')
    AS seed(source_name, watermark_column, last_watermark, status)
) src
ON tgt.source_name = src.source_name
WHEN NOT MATCHED THEN INSERT *;
