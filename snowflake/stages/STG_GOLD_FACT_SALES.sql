CREATE STAGE IF NOT EXISTS SHOPSPHERE_DW.STAGING.STG_GOLD_FACT_SALES
URL = 'azure://adlsgen2shopsphere.blob.core.windows.net/shopsphere/gold/serving/fact_sales/'
STORAGE_INTEGRATION = INT_SHOPSPHERE_ADLS;
