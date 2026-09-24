from pyspark.sql import functions as F
from pyspark.sql.window import Window

shipments = spark.table("SHOPSPHERE.bronze.shipments")
orders = spark.table("SHOPSPHERE.silver.fact_orders").select("order_id").distinct()

valid = shipments.filter(
    F.col("shipment_event_id").isNotNull() &
    F.col("order_id").isNotNull() &
    F.col("event_ts").isNotNull() &
    F.col("status").isNotNull()
)

w = Window.partitionBy("shipment_event_id").orderBy(F.col("event_ts").desc(), F.col("_ingested_at").desc())
latest = valid.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
invalid = latest.join(orders, "order_id", "left_anti")

invalid = invalid.withColumn("error_type", F.lit("REFERENTIAL_INTEGRITY")) \
                 .withColumn("error_reason", F.lit("order_id does not exist in fact_orders"))
invalid.createOrReplaceTempView("invalid_shipment_errors")

spark.sql('''
CREATE TABLE IF NOT EXISTS SHOPSPHERE.error.shipments
USING DELTA
AS SELECT * FROM invalid_shipment_errors WHERE 1 = 0
''')
spark.sql('''
MERGE INTO SHOPSPHERE.error.shipments tgt
USING invalid_shipment_errors src
ON tgt.shipment_event_id = src.shipment_event_id
AND tgt.error_type = src.error_type
AND tgt.error_reason = src.error_reason
WHEN NOT MATCHED THEN INSERT *
''')

clean = latest.join(invalid.select("shipment_event_id"), "shipment_event_id", "left_anti")
clean.createOrReplaceTempView("latest_valid_shipments")

# Initial overwrite cell is RUN ONCE only.
# clean.write...mode("overwrite")...saveAsTable("SHOPSPHERE.silver.fact_shipment_events")

spark.sql('''
MERGE INTO SHOPSPHERE.silver.fact_shipment_events tgt
USING latest_valid_shipments src
ON tgt.shipment_event_id = src.shipment_event_id
WHEN MATCHED AND src.event_ts > tgt.event_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')
