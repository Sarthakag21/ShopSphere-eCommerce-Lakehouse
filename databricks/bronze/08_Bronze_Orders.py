from pyspark.sql import functions as F

order_initial_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/orders/initial/orders_initial.csv"
df_orders = spark.read.format("csv").option("header", True).option("inferSchema", False).load(order_initial_path)

df_bronze_orders = (
    df_orders.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.to_timestamp("order_date").alias("order_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("order_status").cast("string").alias("order_status"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("orders_initial.csv"))
)

# RUN ONCE only. Then permanently SKIP.
(
    df_bronze_orders.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("SHOPSPHERE.bronze.orders")
)
