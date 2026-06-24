# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists capstone;
# MAGIC use catalog capstone;

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

# Define the target database schema name in Databricks
DB_NAME = "capstone_gold_check"

# Create the database inside Databricks metastore if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
print(f"✓ Ensured Databricks database '{DB_NAME}' exists.")

# COMMAND ----------

# MAGIC %md ## Define Tables for Catalog Registration

# COMMAND ----------

TABLES_TO_CATALOG = [
    "dim_customer",
    "dim_product",
    "dim_date",
    "fact_sales",
    "agg_daily_sales_by_store",
    "agg_sales_by_category"
]

# COMMAND ----------

# MAGIC %md ## Save Tables to Catalog

# COMMAND ----------

failures = []

for table_name in TABLES_TO_CATALOG:
    log_pipeline_event("catalog_sync", table_name, "STARTED")
    
    try:
        # 1. Read the Gold Delta table directly from ADLS
        df = read_delta_table("gold", table_name)
        row_count = df.count()
        
        # 2. Write as a managed table in Databricks Local Metastore
        # Overwrite ensures that if you re-run the pipeline, the catalog refreshes cleanly
        (df.write
           .mode("overwrite")
           .saveAsTable(f"{DB_NAME}.{table_name}"))
        
        log_pipeline_event("catalog_sync", table_name, "SUCCESS", records_out=row_count)
        print(f"✓ Successfully registered managed table: {DB_NAME}.{table_name} ({row_count} rows)")
        
    except Exception as e:
        log_pipeline_event("catalog_sync", table_name, "FAILED", error_message=str(e))
        failures.append((table_name, str(e)))
        print(f"✗ Failed to register table {table_name}: {e}")

# COMMAND ----------

# MAGIC %md ## Validation Summary

# COMMAND ----------

if failures:
    summary = "; ".join(f"{t}: {e}" for t, e in failures)
    raise RuntimeError(f"Catalog Sync failed for {len(failures)} table(s): {summary}")
else:
    print(f"\n=== All Gold tables successfully available in Databricks Catalog under '{DB_NAME}' database ===")