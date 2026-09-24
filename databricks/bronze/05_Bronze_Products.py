from pyspark.sql import functions as F

product_initial_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/products/initial/products_initial.csv"
df_products = spark.read.format("csv").option("header", True).option("inferSchema", False).load(product_initial_path)

df_bronze_products = (
    df_products.select(
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("category").cast("string").alias("category"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("currency").cast("string").alias("currency"),
        F.to_date("launch_date").alias("launch_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("products_initial.csv"))
)

# RUN ONCE only. Then permanently SKIP.
(
    df_bronze_products.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("SHOPSPHERE.bronze.products")
)
