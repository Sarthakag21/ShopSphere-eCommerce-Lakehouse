'''Reference initial shipment Bronze load.

The live project numbering includes 15_Bronze_Shipments_AutoLoader and
16_JOB_16_Silver_Shipments_Incremental. This initial source script is
included as the logical one-time Bronze counterpart; export your exact
live notebook if its name/content differs.
'''
from pyspark.sql import functions as F

initial_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/landing/shipments/initial/shipments_initial.csv"
df = spark.read.format("csv").option("header", True).option("inferSchema", False).load(initial_path)

df_bronze = (
    df.select(
        F.col("shipment_event_id").cast("string").alias("shipment_event_id"),
        F.col("order_id").cast("string").alias("order_id"),
        F.to_timestamp("event_ts").alias("event_ts"),
        F.col("status").cast("string").alias("status"),
        F.col("carrier").cast("string").alias("carrier"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("shipments_initial.csv"))
)

# RUN ONCE only.
(
    df_bronze.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("SHOPSPHERE.bronze.shipments")
)
