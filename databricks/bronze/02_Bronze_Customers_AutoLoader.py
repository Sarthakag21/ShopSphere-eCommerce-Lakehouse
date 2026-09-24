from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

source_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/customers/incremental/"
checkpoint_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/checkpoints/customers/"

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])

df_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(customer_schema)
    .option("header", "true")
    .load(source_path)
)

df_bronze_stream = (
    df_stream.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("customer_name").cast("string").alias("customer_name"),
        F.col("email").cast("string").alias("email"),
        F.col("phone").cast("string").alias("phone"),
        F.col("city").cast("string").alias("city"),
        F.col("state").cast("string").alias("state"),
        F.col("country").cast("string").alias("country"),
        F.to_date("signup_date").alias("signup_date"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
)

query = (
    df_bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("SHOPSPHERE.bronze.customers")
)
query.awaitTermination()
