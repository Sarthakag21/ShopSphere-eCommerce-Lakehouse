from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSQLStatementsOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    dag_id="shopsphere_end_to_end",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "shopsphere",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["shopsphere", "ecommerce", "data-engineering"],
)
def shopsphere_pipeline():

    @task
    def start_pipeline():
        print("ShopSphere orchestration started.")

    trigger_adf = AzureDataFactoryRunPipelineOperator(
        task_id="trigger_adf_incremental",
        azure_data_factory_conn_id="azure_data_factory_default",
        resource_group_name="rg-shopsphere-dev",
        factory_name="adf-shopsphere",
        pipeline_name="PL_METADATA_INCREMENTAL_MYSQL_TO_ADLS",
        wait_for_termination=True,
        check_interval=30,
        deferrable=True,
    )

    trigger_databricks = DatabricksRunNowOperator(
        task_id="trigger_databricks_incremental",
        databricks_conn_id="databricks_default",
        job_id=113185099851499,
        wait_for_termination=True,
        polling_period_seconds=30,
        deferrable=True,
    )

    load_snowflake = SQLExecuteQueryOperator(
        task_id="load_snowflake_incremental",
        conn_id="snowflake_default",
        split_statements=True,
        sql='''
        COPY INTO SHOPSPHERE_DW.STAGING.STG_FACT_SALES_INCREMENTAL
        FROM @SHOPSPHERE_DW.STAGING.STG_GOLD_FACT_SALES_INCREMENTAL
        FILE_FORMAT = (
            FORMAT_NAME = 'SHOPSPHERE_DW.STAGING.FF_PARQUET'
        )
        PATTERN = '.*[.]parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

        MERGE INTO SHOPSPHERE_DW.ANALYTICS.FACT_SALES AS tgt
        USING (
            SELECT *
            FROM SHOPSPHERE_DW.STAGING.STG_FACT_SALES_INCREMENTAL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "order_id"
                ORDER BY GREATEST(
                    COALESCE("updated_at", TO_TIMESTAMP_NTZ('1900-01-01')),
                    COALESCE("latest_payment_ts", TO_TIMESTAMP_NTZ('1900-01-01')),
                    COALESCE("latest_shipment_ts", TO_TIMESTAMP_NTZ('1900-01-01')),
                    COALESCE("_ingested_at", TO_TIMESTAMP_NTZ('1900-01-01'))
                ) DESC,
                "_ingested_at" DESC
            ) = 1
        ) AS src
        ON tgt."order_id" = src."order_id"
        WHEN MATCHED THEN UPDATE ALL BY NAME
        WHEN NOT MATCHED THEN INSERT ALL BY NAME;
        ''',
    )

    update_watermarks = DatabricksSQLStatementsOperator(
        task_id="update_pipeline_control",
        databricks_conn_id="databricks_default",
        warehouse_id="74540a1d7dc76de5",
        catalog="SHOPSPHERE",
        schema="gold",
        statement='''
        WITH source_watermarks AS (
            SELECT 'customers' AS source_name, 'effective_from' AS watermark_column,
                   MAX(effective_from) AS last_watermark
            FROM SHOPSPHERE.silver.dim_customers
            WHERE effective_from > (
                SELECT last_watermark FROM SHOPSPHERE.gold.pipeline_control WHERE source_name = 'customers'
            )

            UNION ALL
            SELECT 'products', 'updated_at', MAX(updated_at)
            FROM SHOPSPHERE.silver.dim_products
            WHERE updated_at > (
                SELECT last_watermark FROM SHOPSPHERE.gold.pipeline_control WHERE source_name = 'products'
            )

            UNION ALL
            SELECT 'orders', 'updated_at', MAX(updated_at)
            FROM SHOPSPHERE.silver.fact_orders
            WHERE updated_at > (
                SELECT last_watermark FROM SHOPSPHERE.gold.pipeline_control WHERE source_name = 'orders'
            )

            UNION ALL
            SELECT 'payments', 'payment_ts', MAX(payment_ts)
            FROM SHOPSPHERE.silver.fact_payments
            WHERE payment_ts > (
                SELECT last_watermark FROM SHOPSPHERE.gold.pipeline_control WHERE source_name = 'payments'
            )

            UNION ALL
            SELECT 'shipments', 'event_ts', MAX(event_ts)
            FROM SHOPSPHERE.silver.fact_shipment_events
            WHERE event_ts > (
                SELECT last_watermark FROM SHOPSPHERE.gold.pipeline_control WHERE source_name = 'shipments'
            )
        )
        MERGE INTO SHOPSPHERE.gold.pipeline_control AS tgt
        USING source_watermarks AS src
        ON tgt.source_name = src.source_name
        WHEN MATCHED
             AND src.last_watermark IS NOT NULL
             AND (tgt.last_watermark IS NULL OR src.last_watermark > tgt.last_watermark)
        THEN UPDATE SET
            tgt.last_watermark = src.last_watermark,
            tgt.status = 'SUCCESS';
        ''',
        wait_for_termination=True,
        polling_period_seconds=10,
        deferrable=True,
    )

    @task
    def pipeline_complete():
        print("ShopSphere end-to-end processing completed successfully.")

    start = start_pipeline()
    complete = pipeline_complete()

    start >> trigger_adf >> trigger_databricks >> load_snowflake >> update_watermarks >> complete


shopsphere_pipeline()
