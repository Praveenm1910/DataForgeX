# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze Layer  (ADLS Gen2 Edition)
# MAGIC **Enterprise Retail Analytics Platform on Azure**
# MAGIC
# MAGIC Generic, registry-driven Bronze ingestion.  All `spark.read` and `df.write`
# MAGIC calls inject `.options(**ADLS_OPTS)` for per-operation ADLS authentication.
# MAGIC No `dbutils.widgets`, no Unity Catalog, no `spark.conf.set()`.
# MAGIC
# MAGIC Every table is written as an **External Delta table** at its `abfss://` path.
# MAGIC
# MAGIC Run `01_data_generator` first so there is something to ingest.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

# Dataset filter — edit this variable directly if you want to run a single dataset.
# "ALL" processes every dataset in the registry (default for automated runs).
DATASET_FILTER = "ALL"

datasets_to_process = (
    DATASET_REGISTRY if DATASET_FILTER == "ALL"
    else {DATASET_FILTER: DATASET_REGISTRY[DATASET_FILTER]}
)
print(f"Processing: {list(datasets_to_process.keys())}")

# COMMAND ----------

# MAGIC %md ## Ingestion functions

# COMMAND ----------

def pick_new_files(dataset: str):
    """Raw files not yet recorded in the Bronze file registry."""
    all_files    = list_raw_files(dataset)          # uses binaryFile — no dbutils.fs.ls
    already_done = get_already_ingested_files(dataset)
    return [f for f in all_files if f.path not in already_done]

def ingest_csv_dataset(dataset: str, cfg: dict, file_rows):
    """
    Reads one or more CSV files from ADLS.
    ADLS_OPTS is injected into spark.read so authentication is per-operation.
    """
    paths = [r.path for r in file_rows]

    df = (
        spark.read
             .option("header", True)
             .options(**ADLS_OPTS)
             .csv(paths)
    )

    if cfg.get("bronze_schema"):
        # Typed source (Customers — stands in for a JDBC pull): cast to native SQL types.
        for col_name, target_type in cfg["bronze_schema"].items():
            if col_name in df.columns:
                if target_type == "timestamp":
                    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
                else:
                    df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    else:
        # CSV Bronze Note: every column typed as STRING for structural stability.
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])

    # Fixed: Named the column "_SourceFile" to match downstream expectations 
    # and swapped in the UC-compliant _metadata attribute.
    df = df.withColumn("_SourceFile", F.col("_metadata.file_path"))
    
    return add_bronze_audit_columns(df)



def ingest_json_dataset(dataset: str, cfg: dict, files: list):
    paths = [f.path for f in files]
    
    df = (
        spark.read
             .format("json")
             .option("multiline", "true") 
             .options(**ADLS_OPTS)
             .load(paths)
             # Fixed: Swapped legacy input_file_name() for UC-compliant _metadata
             .withColumn("_SourceFile", F.col("_metadata.file_path")) 
    )
    
    # Route through the audit helper so BOTH _IngestionTimestamp 
    # and _AdfPipelineRunId are applied consistently!
    return add_bronze_audit_columns(df)

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

failures = []

for dataset, cfg in datasets_to_process.items():
    log_pipeline_event("bronze", dataset, "STARTED")
    try:
        new_files = pick_new_files(dataset)
        if not new_files:
            log_pipeline_event("bronze", dataset, "SUCCESS", records_in=0, records_out=0)
            print(f"[skip] {dataset}: no new raw files since last run")
            continue

        write_mode = "overwrite" if cfg["load_pattern"] == "overwrite_latest" else "append"
        if cfg["load_pattern"] == "overwrite_latest":
            # Only the single newest file is authoritative for a full-refresh source.
            files_to_load = [max(new_files, key=lambda f: f.modificationTime)]
        else:
            files_to_load = new_files

        bronze_df = (
            ingest_json_dataset(dataset, cfg, files_to_load) if cfg["is_json"]
            else ingest_csv_dataset(dataset, cfg, files_to_load)
        )

        row_count = bronze_df.count()

        # write_delta_table already injects ADLS_OPTS
        write_delta_table(bronze_df, "bronze", dataset, mode=write_mode)
        register_ingested_files(dataset, new_files)

        log_pipeline_event("bronze", dataset, "SUCCESS", records_in=row_count, records_out=row_count)

    except Exception as e:
        log_pipeline_event("bronze", dataset, "FAILED", error_message=str(e))
        failures.append((dataset, str(e)))

# COMMAND ----------

if failures:
    summary = "; ".join(f"{d}: {e}" for d, e in failures)
    raise RuntimeError(f"Bronze layer failed for {len(failures)} dataset(s): {summary}")
else:
    print("Bronze layer completed successfully for all requested datasets.")

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

for dataset in datasets_to_process:
    if table_exists("bronze", dataset):
        cnt = read_delta_table("bronze", dataset).count()
        print(f"bronze.{dataset}: {cnt} rows")