from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze = spark.table("SHOPSPHERE.bronze.customers")

# Initial history build. RUN ONCE; after the first successful overwrite, permanently SKIP that cell.
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").asc(), F.col("_ingested_at").asc())
initial = (
    bronze
    .withColumn("_next_updated_at", F.lead("updated_at").over(w))
    .select(
        "customer_id", "customer_name", "email", "phone", "city", "state", "country", "signup_date",
        F.col("updated_at").alias("effective_from"),
        F.coalesce(F.col("_next_updated_at"), F.to_timestamp(F.lit("9999-12-31"))).alias("effective_to"),
        (F.col("_next_updated_at").isNull()).alias("is_current"),
    )
)

# RUN ONCE only.
initial.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("SHOPSPHERE.silver.dim_customers")

# Incremental pattern: take the latest source version per key, close current row, insert new row.
latest_w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
latest = bronze.withColumn("rn", F.row_number().over(latest_w)).filter("rn = 1").drop("rn")

latest.createOrReplaceTempView("latest_customer_changes")

spark.sql('''
MERGE INTO SHOPSPHERE.silver.dim_customers AS tgt
USING latest_customer_changes AS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = TRUE
AND tgt.effective_from <=> src.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_id, customer_name, email, phone, city, state, country, signup_date,
          effective_from, effective_to, is_current)
  VALUES (src.customer_id, src.customer_name, src.email, src.phone, src.city, src.state,
          src.country, src.signup_date, src.updated_at, TO_TIMESTAMP('9999-12-31'), TRUE)
''')

# Close prior current versions explicitly before inserting changed versions in a production workflow.
# Use a deterministic null-safe join to the newest source version.
spark.sql('''
MERGE INTO SHOPSPHERE.silver.dim_customers AS tgt
USING latest_customer_changes AS src
ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
WHEN MATCHED AND NOT (
    tgt.customer_name <=> src.customer_name AND
    tgt.email <=> src.email AND
    tgt.phone <=> src.phone AND
    tgt.city <=> src.city AND
    tgt.state <=> src.state AND
    tgt.country <=> src.country AND
    tgt.signup_date <=> src.signup_date
)
THEN UPDATE SET effective_to = src.updated_at, is_current = FALSE
''')

spark.sql('''
INSERT INTO SHOPSPHERE.silver.dim_customers
SELECT src.customer_id, src.customer_name, src.email, src.phone, src.city, src.state, src.country,
       src.signup_date, src.updated_at, TO_TIMESTAMP('9999-12-31'), TRUE
FROM latest_customer_changes src
LEFT JOIN SHOPSPHERE.silver.dim_customers tgt
  ON tgt.customer_id = src.customer_id AND tgt.is_current = TRUE
WHERE tgt.customer_id IS NULL
''')
