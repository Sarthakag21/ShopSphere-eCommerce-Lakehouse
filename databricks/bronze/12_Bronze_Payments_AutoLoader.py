from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

source_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/payments/incremental/"
checkpoint_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/checkpoints/payments/"

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("payment_ts", StringType(), True),
    StructField("payment_status", StringType(), True),
])

df_payments_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(payment_schema)
    .option("header", "true")
    .load(source_path)
)

df_bronze_payments_stream = (
    df_payments_stream.select(
        F.col("payment_id").cast("string").alias("payment_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.col("payment_mode").cast("string").alias("payment_mode"),
        F.to_timestamp("payment_ts").alias("payment_ts"),
        F.col("payment_status").cast("string").alias("payment_status"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
)

query = (
    df_bronze_payments_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("SHOPSPHERE.bronze.payments")
)
query.awaitTermination()
