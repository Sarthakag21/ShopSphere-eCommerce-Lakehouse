from pyspark.sql import functions as F

customer_initial_path = (
    "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/"
    "landing/customers/initial/customers_initial.csv"
)

df_customers = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(customer_initial_path)
)

df_bronze_customers = (
    df_customers.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("customer_name").cast("string").alias("customer_name"),
        F.col("email").cast("string").alias("email"),
        F.col("phone").cast("string").alias("phone"),
        F.col("city").cast("string").alias("city"),
        F.col("state").cast("string").alias("state"),
        F.col("country").cast("string").alias("country"),
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("customers_initial.csv"))
)

# RUN ONCE only. Then permanently SKIP this write block.
(
    df_bronze_customers.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("SHOPSPHERE.bronze.customers")
)
