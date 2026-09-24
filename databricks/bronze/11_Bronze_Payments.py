from pyspark.sql import functions as F

initial_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/payments/initial/payments_initial.csv"
df_payments = spark.read.format("csv").option("header", True).option("inferSchema", False).load(initial_path)

df_bronze_payments = (
    df_payments.select(
        F.col("payment_id").cast("string").alias("payment_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("payment_mode").cast("string").alias("payment_mode"),
        F.to_timestamp("payment_ts").alias("payment_ts"),
        F.col("payment_status").cast("string").alias("payment_status"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("payments_initial.csv"))
)

# RUN ONCE only. Then permanently SKIP.
(
    df_bronze_payments.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("SHOPSPHERE.bronze.payments")
)
