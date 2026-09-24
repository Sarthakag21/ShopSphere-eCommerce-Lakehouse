# Scenario-Based Interview Notes

## Bronze succeeded but Silver failed

Airflow/Databricks dependency prevents Gold from running. Investigate the Silver failure, correct the data/code/resource issue, and rerun only the failed/current downstream task according to the job's retry semantics. Bronze should remain append-oriented.

## Duplicate source records

Deduplicate using a business key plus ordering timestamp and ingestion timestamp, e.g. `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, _ingested_at DESC)`.

## One executor receives most data

Inspect skewed join keys, use salting for severe skew, broadcast a small dimension when appropriate, and consider AQE/skew join handling.

## Pipeline suddenly becomes slow

Compare input volume, file sizes, partitions, skew, Spark UI stages, shuffle read/write, task duration distribution, and whether new columns or joins changed the physical plan.

## Watermark safety

Never update the source watermark before downstream processing succeeds. In ShopSphere the watermark is advanced only after Snowflake COPY + MERGE completes successfully.
