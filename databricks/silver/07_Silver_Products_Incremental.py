from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze = spark.table("SHOPSPHERE.bronze.products")
validated = bronze.filter(
    F.col("product_id").isNotNull() &
    F.col("product_name").isNotNull() &
    F.col("category").isNotNull() &
    F.col("price").isNotNull() & (F.col("price") >= 0) &
    F.col("currency").isNotNull() &
    F.col("launch_date").isNotNull() &
    F.col("updated_at").isNotNull()
)

w = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
latest = validated.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

# Initial overwrite cell is RUN ONCE only.
# latest.write...mode("overwrite")...saveAsTable("SHOPSPHERE.silver.dim_products")

latest.createOrReplaceTempView("latest_products")

spark.sql('''
MERGE INTO SHOPSPHERE.silver.dim_products tgt
USING latest_products src
ON tgt.product_id = src.product_id
WHEN MATCHED AND src.updated_at > tgt.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')
