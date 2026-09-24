from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

source_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/shipments/incremental/"
checkpoint_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/checkpoints/shipments/"

schema = StructType([
    StructField("shipment_event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("status", StringType(), True),
    StructField("carrier", StringType(), True),
])

df_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(schema)
    .option("header", "true")
    .load(source_path)
)

df_bronze_stream = (
    df_stream.select(
        F.col("shipment_event_id").cast("string").alias("shipment_event_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("status").cast("string").alias("status"),
        F.col("carrier").cast("string").alias("carrier"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
)

query = (
    df_bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("SHOPSPHERE.bronze.shipments")
)
query.awaitTermination()
