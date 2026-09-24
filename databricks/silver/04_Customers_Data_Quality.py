from pyspark.sql import functions as F

bronze = spark.table("SHOPSPHERE.bronze.customers")

validated = (
    bronze
    .withColumn("dq_reason", F.concat_ws("; ",
        F.when(F.col("customer_id").isNull(), "customer_id is null"),
        F.when(F.col("customer_name").isNull(), "customer_name is null"),
        F.when(F.col("email").isNull(), "email is null"),
        F.when(~F.col("email").rlike(r"^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"), "invalid email"),
        F.when(F.col("country").isNull(), "country is null"),
        F.when(F.col("signup_date").isNull(), "signup_date is null"),
        F.when(F.col("updated_at").isNull(), "updated_at is null"),
        F.when(F.col("signup_date") > F.current_date(), "signup_date is future"),
    ))
    .withColumn("dq_pass", F.col("dq_reason") == "")
    .withColumn("dq_record_id", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in bronze.columns]), 256))
)

valid = validated.filter("dq_pass").drop("dq_reason", "dq_pass", "dq_record_id")
invalid = validated.filter("NOT dq_pass").withColumn("error_type", F.lit("DATA_QUALITY"))

valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("SHOPSPHERE.silver.stg_customers_valid")

invalid.createOrReplaceTempView("invalid_customers")
spark.sql('''
CREATE TABLE IF NOT EXISTS SHOPSPHERE.error.customers
USING DELTA
AS SELECT * FROM invalid_customers WHERE 1 = 0
''')
spark.sql('''
MERGE INTO SHOPSPHERE.error.customers tgt
USING invalid_customers src
ON tgt.dq_record_id = src.dq_record_id
WHEN NOT MATCHED THEN INSERT *
''')
