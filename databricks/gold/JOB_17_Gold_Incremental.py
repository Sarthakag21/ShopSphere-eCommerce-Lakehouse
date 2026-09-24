from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = spark.table("SHOPSPHERE.silver.fact_orders")
customers = spark.table("SHOPSPHERE.silver.dim_customers").filter("is_current = true")
products = spark.table("SHOPSPHERE.silver.dim_products")
payments = spark.table("SHOPSPHERE.silver.fact_payments")
shipments = spark.table("SHOPSPHERE.silver.fact_shipment_events")

# Current payment summary per order.
payment_summary = (
    payments.groupBy("order_id")
    .agg(
        F.sum("amount").alias("total_paid"),
        F.max("payment_ts").alias("latest_payment_ts"),
    )
)

# Latest shipment event per order.
ship_w = Window.partitionBy("order_id").orderBy(F.col("event_ts").desc(), F.col("_ingested_at").desc())
latest_ship = (
    shipments.withColumn("rn", F.row_number().over(ship_w))
    .filter("rn = 1")
    .select(
        "order_id",
        F.col("status").alias("shipment_status"),
        F.col("carrier").alias("shipment_carrier"),
        F.col("event_ts").alias("latest_shipment_ts"),
    )
)

fact = (
    orders.alias("o")
    .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .join(products.alias("p"), F.col("o.product_id") == F.col("p.product_id"), "left")
    .join(payment_summary.alias("pay"), F.col("o.order_id") == F.col("pay.order_id"), "left")
    .join(latest_ship.alias("sh"), F.col("o.order_id") == F.col("sh.order_id"), "left")
    .select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.order_date").alias("order_date"),
        F.col("o.order_status").alias("order_status"),
        F.col("o.customer_id").alias("customer_id"),
        F.col("c.customer_name").alias("customer_name"),
        F.col("c.city").alias("city"),
        F.col("c.state").alias("state"),
        F.col("c.country").alias("country"),
        F.col("o.product_id").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.category").alias("category"),
        F.col("p.price").alias("unit_price"),
        F.col("o.quantity").alias("quantity"),
        (F.col("p.price") * F.col("o.quantity")).cast("decimal(24,2)").alias("gross_revenue"),
        F.coalesce(F.col("pay.total_paid"), F.lit(0)).cast("decimal(22,2)").alias("total_paid"),
        F.col("pay.latest_payment_ts").alias("latest_payment_ts"),
        F.col("sh.shipment_status").alias("shipment_status"),
        F.col("sh.shipment_carrier").alias("shipment_carrier"),
        F.col("sh.latest_shipment_ts").alias("latest_shipment_ts"),
        F.greatest(
            F.col("o.updated_at"),
            F.col("c.effective_from"),
            F.col("p.updated_at"),
            F.col("pay.latest_payment_ts"),
            F.col("sh.latest_shipment_ts"),
        ).alias("updated_at"),
        F.current_timestamp().alias("_ingested_at"),
    )
)

# Main Gold Delta table. Create once, then use incremental MERGE or overwrite of only the controlled Gold export as designed in the live job.
fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("SHOPSPHERE.gold.fact_sales")

# Export the business-ready fact to ADLS as Parquet for Snowflake COPY INTO.
incremental_gold_path = "abfss://shopsphere@adlsgen2shopsphere.dfs.core.windows.net/gold/serving/fact_sales_incremental/"
(
    fact.write.format("parquet")
    .mode("append")
    .save(incremental_gold_path)
)
