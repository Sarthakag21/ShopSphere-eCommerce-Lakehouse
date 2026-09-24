from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders = spark.table("SHOPSPHERE.bronze.orders")
customers = spark.table("SHOPSPHERE.silver.dim_customers").filter("is_current = true")
products = spark.table("SHOPSPHERE.silver.dim_products")

valid = orders.filter(
    F.col("order_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    F.col("product_id").isNotNull() &
    F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
    F.col("order_date").isNotNull() &
    F.col("updated_at").isNotNull() &
    F.col("order_status").isNotNull()
)

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
latest = valid.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

# Initial overwrite cell is RUN ONCE only.
# latest.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("SHOPSPHERE.silver.fact_orders")

invalid_customer = latest.join(customers.select("customer_id"), "customer_id", "left_anti")
invalid_product = latest.join(products.select("product_id"), "product_id", "left_anti")
invalid = (
    invalid_customer.select("order_id").withColumn("error_type", F.lit("REFERENTIAL_INTEGRITY"))
    .withColumn("error_reason", F.lit("customer_id does not exist"))
    .unionByName(
        invalid_product.select("order_id").withColumn("error_type", F.lit("REFERENTIAL_INTEGRITY"))
        .withColumn("error_reason", F.lit("product_id does not exist"))
    )
)

invalid.createOrReplaceTempView("invalid_order_errors")
spark.sql('''
CREATE TABLE IF NOT EXISTS SHOPSPHERE.error.orders
USING DELTA
AS SELECT * FROM invalid_order_errors WHERE 1 = 0
''')
spark.sql('''
MERGE INTO SHOPSPHERE.error.orders tgt
USING invalid_order_errors src
ON tgt.order_id = src.order_id
AND tgt.error_type = src.error_type
AND tgt.error_reason = src.error_reason
WHEN NOT MATCHED THEN INSERT *
''')

clean = latest.join(invalid.select("order_id").distinct(), "order_id", "left_anti")
clean.createOrReplaceTempView("latest_valid_orders")

spark.sql('''
MERGE INTO SHOPSPHERE.silver.fact_orders tgt
USING latest_valid_orders src
ON tgt.order_id = src.order_id
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')
