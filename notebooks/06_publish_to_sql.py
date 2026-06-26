# Databricks notebook source
# MAGIC %run ./00_config_utils

# COMMAND ----------

# Attempt to load SQL credentials from the config. 
# Providing safe fallbacks so the script compiles even if the DB isn't spun up yet.
SQL_CFG = _CFG.get("azure_sql", {})

SQL_SERVER   = SQL_CFG.get("server_name", "YOUR_SERVER.database.windows.net")
SQL_DB       = SQL_CFG.get("database_name", "YOUR_DB")
SQL_USER     = SQL_CFG.get("username", "YOUR_USER")
SQL_PASSWORD = SQL_CFG.get("password", "YOUR_PASSWORD")

# Standard Azure SQL JDBC connection string
JDBC_URL = f"jdbc:sqlserver://{SQL_SERVER}:1433;database={SQL_DB};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# MAGIC %md ## Define Tables for Export

# COMMAND ----------

TABLES_TO_PUBLISH = SQL_CFG.get("tables_to_publish", [])
print(TABLES_TO_PUBLISH)

# COMMAND ----------

# MAGIC %md ## Execute Export

# COMMAND ----------

failures = []

for table_name in TABLES_TO_PUBLISH:
    # We log this just like the Bronze/Silver/Gold transformations
    log_pipeline_event("sql_export", table_name, "STARTED")
    
    try:
        # 1. Read the Gold Delta table directly from ADLS
        df = read_delta_table("gold", table_name)
        row_count = df.count()
        
        # 2. Write to Azure SQL using JDBC
        # We use mode("overwrite") so the reporting layer always mirrors the Lakehouse exactly,
        # avoiding duplicate records in the presentation layer.
        (df.write
           .format("jdbc")
           .option("url", JDBC_URL)
           .option("dbtable", f"dbo.{table_name}") 
           .option("user", SQL_USER)
           .option("password", SQL_PASSWORD)
           .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
           # Recommended for bulk loading into Azure SQL:
           .option("batchsize", "10000") 
           .option("tableLock", "true")
           .mode("overwrite")
           .save())
        
        log_pipeline_event("sql_export", table_name, "SUCCESS", records_out=row_count)
        print(f"✓ Successfully published dbo.{table_name} ({row_count} rows)")
        
    except Exception as e:
        log_pipeline_event("sql_export", table_name, "FAILED", error_message=str(e))
        failures.append((table_name, str(e)))
        print(f"✗ Failed to publish {table_name}: {e}")

# COMMAND ----------

# MAGIC %md ## Validation

# COMMAND ----------

if failures:
    summary = "; ".join(f"{t}: {e}" for t, e in failures)
    raise RuntimeError(f"SQL Export failed for {len(failures)} table(s): {summary}")
else:
    print("\n=== All Gold tables successfully published to Azure SQL ===")