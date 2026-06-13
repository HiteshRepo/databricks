# Databricks notebook source
# Day 1 — Delta Lake Deep Dive + Change Data Feed
# Catalog: retailflow | Schemas: raw, silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

CATALOG       = "retailflow"
RAW_SCHEMA    = f"{CATALOG}.raw"
SILVER_SCHEMA = f"{CATALOG}.silver"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create base tables + insert seed data
# MAGIC
# MAGIC We create two Delta tables manually (no Auto Loader yet — that's Day 3).
# MAGIC `customers` will be the main subject for MERGE, time travel, and CDF.
# MAGIC `products` is used for the schema-evolution section.

# COMMAND ----------

# Drop and recreate for a clean run
spark.sql(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.customers")
spark.sql(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.products")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {RAW_SCHEMA}.customers (
  customer_id   INT,
  name          STRING,
  email         STRING,
  region        STRING,
  updated_at    TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'false'   -- we'll enable this in section 5
)
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO {RAW_SCHEMA}.customers VALUES
  (1,  'Alice Nguyen',   'alice@retailflow.io',  'WEST',  '2024-01-01 08:00:00'),
  (2,  'Bob Patel',      'bob@retailflow.io',    'EAST',  '2024-01-01 08:00:00'),
  (3,  'Carol Smith',    'carol@retailflow.io',  'NORTH', '2024-01-01 08:00:00'),
  (4,  'David Kim',      'david@retailflow.io',  'SOUTH', '2024-01-01 08:00:00'),
  (5,  'Eva Torres',     'eva@retailflow.io',    'WEST',  '2024-01-01 08:00:00')
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {RAW_SCHEMA}.products (
  product_id    INT,
  name          STRING,
  price         DECIMAL(10,2),
  updated_at    TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
INSERT INTO {RAW_SCHEMA}.products VALUES
  (101, 'Widget A', 9.99,  '2024-01-01 08:00:00'),
  (102, 'Widget B', 14.99, '2024-01-01 08:00:00'),
  (103, 'Widget C', 4.99,  '2024-01-01 08:00:00')
""")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {RAW_SCHEMA}.customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MERGE — all clause variants
# MAGIC
# MAGIC ### Exam focus
# MAGIC - `WHEN NOT MATCHED BY SOURCE` — **deletes** target rows absent from the source.
# MAGIC   This is NOT the same as a plain DELETE; it fires per-row within the MERGE transaction.
# MAGIC - All three clauses can coexist in a single MERGE statement.
# MAGIC - MERGE is atomic: all inserts/updates/deletes commit together in one transaction log entry.

# COMMAND ----------

# Incoming batch — simulates a CDC feed:
#   customer 1 : email changed  → WHEN MATCHED UPDATE
#   customer 6 : brand new      → WHEN NOT MATCHED INSERT
#   customers 4 & 5 are absent  → WHEN NOT MATCHED BY SOURCE DELETE
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from datetime import datetime

source_data = [
    Row(customer_id=1, name="Alice Nguyen",  email="alice.new@retailflow.io", region="WEST",  updated_at=datetime(2024, 1, 2, 9, 0, 0)),
    Row(customer_id=2, name="Bob Patel",     email="bob@retailflow.io",       region="EAST",  updated_at=datetime(2024, 1, 2, 9, 0, 0)),
    Row(customer_id=3, name="Carol Smith",   email="carol@retailflow.io",     region="NORTH", updated_at=datetime(2024, 1, 2, 9, 0, 0)),
    Row(customer_id=6, name="Frank Lee",     email="frank@retailflow.io",     region="EAST",  updated_at=datetime(2024, 1, 2, 9, 0, 0)),
]

source_df = spark.createDataFrame(source_data)
source_df.createOrReplaceTempView("customers_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full three-clause MERGE
# MAGIC MERGE INTO retailflow.raw.customers AS t
# MAGIC USING customers_source AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC
# MAGIC -- Clause 1: update changed rows that exist in source
# MAGIC WHEN MATCHED AND t.updated_at < s.updated_at THEN
# MAGIC   UPDATE SET
# MAGIC     t.email      = s.email,
# MAGIC     t.updated_at = s.updated_at
# MAGIC
# MAGIC -- Clause 2: insert rows that are new in source
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, name, email, region, updated_at)
# MAGIC   VALUES (s.customer_id, s.name, s.email, s.region, s.updated_at)
# MAGIC
# MAGIC -- Clause 3: delete target rows not present in source at all
# MAGIC -- Exam trap: this clause only exists in Delta Lake (not standard SQL MERGE)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: alice has new email, frank added, david/eva deleted
# MAGIC SELECT * FROM retailflow.raw.customers ORDER BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The MERGE shows as a single version in the transaction log
# MAGIC DESCRIBE HISTORY retailflow.raw.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key exam points on MERGE
# MAGIC - `WHEN NOT MATCHED BY SOURCE` requires Delta Lake — it's not in ANSI SQL.
# MAGIC - You can have multiple `WHEN MATCHED` clauses with different conditions; only the **first** matching clause fires per row.
# MAGIC - MERGE acquires a write lock on the target table but reads source optimistically.
# MAGIC - `operationMetrics` in DESCRIBE HISTORY shows `numTargetRowsInserted`, `numTargetRowsUpdated`, `numTargetRowsDeleted`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema evolution with mergeSchema
# MAGIC
# MAGIC ### Exam focus
# MAGIC - `mergeSchema = true` — **additive only**: new columns in the source are added to the target schema; existing columns are untouched.
# MAGIC - `overwriteSchema = true` — **destructive**: replaces the entire schema; use with `mode("overwrite")`. Requires `spark.databricks.delta.schema.autoMerge.enabled = true` **or** the explicit option.
# MAGIC - Neither flag changes existing column types (that would require a full rewrite).

# COMMAND ----------

# Source with a new column `loyalty_tier` not in the target
from pyspark.sql.functions import lit

evolved_source = spark.createDataFrame([
    Row(customer_id=7, name="Grace Hall", email="grace@retailflow.io", region="WEST", updated_at=datetime(2024, 1, 3, 10, 0, 0)),
], schema="customer_id INT, name STRING, email STRING, region STRING, updated_at TIMESTAMP") \
    .withColumn("loyalty_tier", lit("GOLD"))

evolved_source.createOrReplaceTempView("customers_evolved")

# COMMAND ----------

# DBR 13+ behaviour: INSERT * in MERGE silently drops extra source columns not in the target.
# loyalty_tier is ignored and grace is inserted without it — no error raised.
# To trigger a schema mismatch error, explicitly reference the unknown column by name:
#
#   WHEN NOT MATCHED THEN
#     INSERT (customer_id, name, email, region, updated_at, loyalty_tier)
#     VALUES (s.customer_id, s.name, s.email, s.region, s.updated_at, s.loyalty_tier)
#
# That will fail with AnalysisException because loyalty_tier doesn't exist in the target yet.
spark.sql(f"""
    MERGE INTO {RAW_SCHEMA}.customers AS t
    USING customers_evolved AS s
    ON t.customer_id = s.customer_id
    WHEN NOT MATCHED THEN
      INSERT *
""")

# COMMAND ----------

# Grace was already inserted by the INSERT * above (without loyalty_tier).
# Delete her so the MERGE WITH SCHEMA EVOLUTION can re-insert her with loyalty_tier = GOLD.
spark.sql(f"DELETE FROM {RAW_SCHEMA}.customers WHERE customer_id = 7")

# On serverless, spark.conf.set for Delta schema settings is blocked entirely.
# Use WITH SCHEMA EVOLUTION in the MERGE statement instead (DBR 15.2+).
# This is scoped to the single statement — no reset needed, nothing leaks globally.
spark.sql(f"""
    MERGE WITH SCHEMA EVOLUTION INTO {RAW_SCHEMA}.customers AS t
    USING customers_evolved AS s
    ON t.customer_id = s.customer_id
    WHEN NOT MATCHED THEN
      INSERT *
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- loyalty_tier column now exists; existing rows have NULL for it
# MAGIC SELECT * FROM retailflow.raw.customers ORDER BY customer_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### overwriteSchema — full schema replacement
# MAGIC
# MAGIC Use when you need to **change column types or drop columns**.
# MAGIC This always creates a new Delta snapshot (version N+1); old data files are not
# MAGIC rewritten until VACUUM runs.

# COMMAND ----------

replacement_df = spark.createDataFrame([
    (101, "Widget A", 11.99, "ACTIVE"),
    (102, "Widget B", 16.99, "ACTIVE"),
], schema="product_id INT, name STRING, price DOUBLE, status STRING")  # price changed type, category removed, status added

replacement_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{RAW_SCHEMA}.products")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE retailflow.raw.products  -- updated_at gone, status added, price is now DOUBLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Time travel
# MAGIC
# MAGIC ### Exam focus
# MAGIC - Delta stores every table version in the transaction log (`_delta_log/`).
# MAGIC - You can query by **version number** or **timestamp**.
# MAGIC - `RESTORE` rolls the table back (adds a new version — does NOT delete history).
# MAGIC - Default retention is **30 days** for the log; file retention is controlled by `delta.logRetentionDuration`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full audit trail for customers
# MAGIC DESCRIBE HISTORY retailflow.raw.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 0 = CREATE TABLE (no rows). Version 1 = seed INSERT — the original five rows.
# MAGIC SELECT * FROM retailflow.raw.customers VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query by timestamp (use a real timestamp from DESCRIBE HISTORY above)
# MAGIC -- SELECT * FROM retailflow.raw.customers TIMESTAMP AS OF '2024-01-01 08:00:00'

# COMMAND ----------

# PySpark equivalent — useful inside ETL notebooks
v0_df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table(f"{RAW_SCHEMA}.customers")

display(v0_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RESTORE to version 0 — rolls back the table; adds a new version entry
# MAGIC -- Exam note: RESTORE is NOT a DELETE of history; it just creates version N+1 that mirrors v0
# MAGIC -- RESTORE TABLE retailflow.raw.customers TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaction log internals (exam context)
# MAGIC
# MAGIC ```
# MAGIC _delta_log/
# MAGIC   00000000000000000000.json   ← CREATE TABLE
# MAGIC   00000000000000000001.json   ← INSERT (seed data)
# MAGIC   00000000000000000002.json   ← MERGE (update + insert + delete)
# MAGIC   00000000000000000003.json   ← MERGE with schema evolution
# MAGIC   ...
# MAGIC   00000000000000000010.checkpoint.parquet  ← every 10 commits
# MAGIC ```
# MAGIC
# MAGIC - Each `.json` entry records `add` / `remove` file actions and stats.
# MAGIC - **Optimistic concurrency**: two writers read the same version, attempt to commit — the second commit succeeds only if the files it would affect weren't touched by the first (conflict detection on overlapping predicates).
# MAGIC - If conflicts exist, Delta raises `ConcurrentModificationException`; the caller must retry.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Change Data Feed — enable, write, read table_changes()
# MAGIC
# MAGIC ### Exam focus
# MAGIC - CDF must be **explicitly enabled** per table — it is off by default.
# MAGIC - After enabling, row-level change records (`_change_type`, `_commit_version`, `_commit_timestamp`) are written for every DML operation.
# MAGIC - Read with `readChangeFeed = true` plus **either** `startingVersion` (inclusive) **or** `startingTimestamp`.
# MAGIC - Change types: `insert`, `update_preimage`, `update_postimage`, `delete`.
# MAGIC - CDF is the foundation for incremental Silver→Gold propagation and DLT `APPLY CHANGES`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable CDF on customers (can also be set at CREATE TABLE time)
# MAGIC ALTER TABLE retailflow.raw.customers
# MAGIC   SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Capture current version — CDF only records changes AFTER enablement
# MAGIC DESCRIBE HISTORY retailflow.raw.customers LIMIT 1

# COMMAND ----------

# Note the version number from the cell above; CDF starts from the NEXT write.
# Let's record it programmatically:
cdf_start_version = spark.sql(
    f"DESCRIBE HISTORY {RAW_SCHEMA}.customers LIMIT 1"
).select("version").first()[0]

print(f"CDF will capture changes from version > {cdf_start_version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write 1: update two customers
# MAGIC UPDATE retailflow.raw.customers
# MAGIC SET    email = 'alice.v2@retailflow.io', updated_at = '2024-02-01 10:00:00'
# MAGIC WHERE  customer_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write 2: delete one customer
# MAGIC DELETE FROM retailflow.raw.customers WHERE customer_id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write 3: insert a new customer
# MAGIC INSERT INTO retailflow.raw.customers VALUES
# MAGIC   (8, 'Henry Wu', 'henry@retailflow.io', 'NORTH', '2024-02-01 11:00:00', NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the change feed

# COMMAND ----------

# Read all changes since CDF was enabled
changes_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", cdf_start_version + 1) \
    .table(f"{RAW_SCHEMA}.customers")

display(changes_df.orderBy("_commit_version", "customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpreting the output
# MAGIC
# MAGIC | `_change_type`      | Meaning |
# MAGIC |---------------------|---------|
# MAGIC | `insert`            | New row added |
# MAGIC | `update_preimage`   | Row **before** the update — use to audit what changed |
# MAGIC | `update_postimage`  | Row **after** the update — use as the new value |
# MAGIC | `delete`            | Row that was deleted — the last known state |
# MAGIC
# MAGIC **Exam trap**: an UPDATE generates **two** CDF rows per affected row (pre + post).
# MAGIC When building a Silver table from CDF, filter to `update_postimage` and `insert` only
# MAGIC (unless you need the audit trail).

# COMMAND ----------

# Practical pattern: apply CDF changes incrementally to a Silver table
from pyspark.sql.functions import col

# Simulate downstream consumer — only care about current state
current_state_df = changes_df.filter(
    col("_change_type").isin("insert", "update_postimage")
).drop("_change_type", "_commit_version", "_commit_timestamp")

display(current_state_df)

# COMMAND ----------

# Read CDF by timestamp instead of version (useful when version is unknown)
# from datetime import datetime, timedelta
# changes_by_ts = spark.read.format("delta") \
#     .option("readChangeFeed", "true") \
#     .option("startingTimestamp", "2024-02-01 00:00:00") \
#     .table(f"{RAW_SCHEMA}.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming CDF (exam-relevant)
# MAGIC
# MAGIC CDF can also be consumed as a streaming source — critical for incremental pipelines:
# MAGIC
# MAGIC ```python
# MAGIC stream_df = spark.readStream.format("delta") \
# MAGIC     .option("readChangeFeed", "true") \
# MAGIC     .option("startingVersion", 0) \
# MAGIC     .table("retailflow.raw.customers")
# MAGIC ```
# MAGIC
# MAGIC - Each micro-batch receives only new changes since the last checkpoint.
# MAGIC - This is how DLT `APPLY CHANGES INTO` works under the hood.
# MAGIC - Do NOT use `readChangeFeed` with `ignoreChanges` or `ignoreDeletes` — they are mutually exclusive options.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Day 1 exam quick-reference
# MAGIC
# MAGIC | Topic | Key point |
# MAGIC |-------|-----------|
# MAGIC | MERGE clauses | `WHEN NOT MATCHED BY SOURCE` deletes target rows absent from source — Delta-only |
# MAGIC | MERGE ordering | Multiple `WHEN MATCHED` clauses: **first match wins** per row |
# MAGIC | mergeSchema | Additive only — new columns added, existing untouched |
# MAGIC | overwriteSchema | Replaces entire schema; requires `mode("overwrite")` |
# MAGIC | Time travel | `VERSION AS OF n` or `TIMESTAMP AS OF ts`; RESTORE adds a new version |
# MAGIC | Optimistic concurrency | Conflict = overlapping predicates between concurrent writers → `ConcurrentModificationException` |
# MAGIC | CDF enablement | Off by default; set `delta.enableChangeDataFeed = true` |
# MAGIC | CDF change types | insert / update_preimage / update_postimage / delete |
# MAGIC | CDF read options | `readChangeFeed` + (`startingVersion` OR `startingTimestamp`) |
# MAGIC | CDF for streaming | Use `readStream` — each batch gets only new changes since last checkpoint |
