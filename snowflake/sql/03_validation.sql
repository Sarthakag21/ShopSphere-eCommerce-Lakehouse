SELECT COUNT(*) AS total_rows
FROM SHOPSPHERE_DW.ANALYTICS.FACT_SALES;

SELECT
    "order_id",
    "order_status",
    "total_paid",
    "latest_payment_ts",
    "shipment_status",
    "shipment_carrier"
FROM SHOPSPHERE_DW.ANALYTICS.FACT_SALES
WHERE "order_id" = '00000001';

SELECT COUNT(*) AS staged_rows
FROM SHOPSPHERE_DW.STAGING.STG_FACT_SALES_INCREMENTAL;
