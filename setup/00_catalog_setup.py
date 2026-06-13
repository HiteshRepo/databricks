# Databricks notebook source
# Run this once in your Databricks workspace to bootstrap the RetailFlow project.
# Requires: Unity Catalog enabled, admin or catalog-creation privileges.

# COMMAND ----------

# MAGIC %md
# MAGIC # RetailFlow — Catalog & Schema Setup
# MAGIC Creates the `retailflow` catalog, schemas, and volumes used across all 10 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: this metastore has no default storage root, so MANAGED LOCATION is required.
# MAGIC -- Storage root is the workspace Unity Catalog ADLS container.
# MAGIC CREATE CATALOG IF NOT EXISTS retailflow
# MAGIC   MANAGED LOCATION '$CATALOG_STORAGE_ROOT'
# MAGIC   COMMENT 'RetailFlow study project — Databricks DE Professional exam prep';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS retailflow.raw
# MAGIC COMMENT 'Landing zone: raw Delta tables and Auto Loader targets';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retailflow.silver
# MAGIC COMMENT 'Validated and deduplicated data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS retailflow.gold
# MAGIC COMMENT 'Aggregated, business-ready tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volumes act as the managed file storage layer inside Unity Catalog.
# MAGIC -- /raw/landing  -> drop JSON files here for Auto Loader to pick up
# MAGIC -- /raw/checkpoints -> streaming checkpoint location
# MAGIC CREATE VOLUME IF NOT EXISTS retailflow.raw.landing
# MAGIC COMMENT 'Landing zone for incoming JSON files (customers, products, orders)';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS retailflow.raw.checkpoints
# MAGIC COMMENT 'Streaming checkpoint storage';

# COMMAND ----------

# Verify everything was created
display(spark.sql("SHOW SCHEMAS IN retailflow"))

# COMMAND ----------

display(spark.sql("SHOW VOLUMES IN retailflow.raw"))

# COMMAND ----------

# Print the volume paths you will use in all subsequent notebooks
landing   = "/Volumes/retailflow/raw/landing"
checkpoints = "/Volumes/retailflow/raw/checkpoints"

print(f"Landing zone : {landing}")
print(f"  customers  : {landing}/customers/")
print(f"  products   : {landing}/products/")
print(f"  orders     : {landing}/orders/")
print(f"Checkpoints  : {checkpoints}")
