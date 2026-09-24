from pyspark.sql import functions as F
from pyspark.sql.window import Window

payments = spark.table("SHOPSPHERE.bronze.payments")
orders = spark.table("SHOPSPHERE.silver.fact_orders").select("order_id").distinct()

valid = payments.filter(
    F.col("payment_id").isNotNull() & F.col("order_id").isNotNull() &
    F.col("amount").isNotNull() & (F.col("amount") > 0) &
    F.col("payment_mode").isNotNull() & F.col("payment_ts").isNotNull() &
    F.col("payment_status").isNotNull()
)

w = Window.partitionBy("payment_id").orderBy(F.col("payment_ts").desc(), F.col("_ingested_at").desc())
latest = valid.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

invalid = latest.join(orders, "order_id", "left_anti")
invalid = invalid.withColumn("error_type", F.lit("REFERENTIAL_INTEGRITY")) \
                 .withColumn("error_reason", F.lit("order_id does not exist in fact_orders"))
invalid.createOrReplaceTempView("invalid_payment_errors")

spark.sql('''
CREATE TABLE IF NOT EXISTS SHOPSPHERE.error.payments
USING DELTA
AS SELECT * FROM invalid_payment_errors WHERE 1 = 0
''')
spark.sql('''
MERGE INTO SHOPSPHERE.error.payments tgt
USING invalid_payment_errors src
ON tgt.payment_id = src.payment_id
AND tgt.error_type = src.error_type
AND tgt.error_reason = src.error_reason
WHEN NOT MATCHED THEN INSERT *
''')

clean = latest.join(invalid.select("payment_id"), "payment_id", "left_anti")
clean.createOrReplaceTempView("latest_valid_payments")

# Initial overwrite cell is RUN ONCE only.
# clean.write...mode("overwrite")...saveAsTable("SHOPSPHERE.silver.fact_payments")

spark.sql('''
MERGE INTO SHOPSPHERE.silver.fact_payments tgt
USING latest_valid_payments src
ON tgt.payment_id = src.payment_id
WHEN MATCHED AND src.payment_ts > tgt.payment_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')
