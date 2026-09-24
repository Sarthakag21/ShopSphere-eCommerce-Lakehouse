from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

source_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/products/incremental/"
checkpoint_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/checkpoints/products_v2/"

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("launch_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])

df_products_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(product_schema)
    .option("header", "true")
    .load(source_path)
)

df_bronze_products_stream = (
    df_products_stream.select(
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("category").cast("string").alias("category"),
        F.col("price").cast("decimal(12,2)").alias("price"),
        F.col("currency").cast("string").alias("currency"),
        F.to_date("launch_date").alias("launch_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
)

query = (
    df_bronze_products_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("SHOPSPHERE.bronze.products")
)
query.awaitTermination()
