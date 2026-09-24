from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

source_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/orders/incremental/"
checkpoint_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/checkpoints/orders/"

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("order_status", StringType(), True),
])

df_orders_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(order_schema)
    .option("header", "true")
    .load(source_path)
)

df_bronze_orders_stream = (
    df_orders_stream.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.to_timestamp("order_date").alias("order_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.col("order_status").cast("string").alias("order_status"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
)

query = (
    df_bronze_orders_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("SHOPSPHERE.bronze.orders")
)
query.awaitTermination()
