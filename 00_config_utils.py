# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone Config & Utilities (ADLS Gen2 Edition)
# MAGIC **Enterprise Retail Analytics Platform on Azure**
# MAGIC
# MAGIC Reads all configuration from `config.json` (no `dbutils.widgets`).
# MAGIC Every Delta table is written as an **External Delta table** directly to an `abfss://` path.
# MAGIC Unity Catalog and `dbutils.fs` are not used anywhere in this file.
# MAGIC
# MAGIC Import from any other notebook with:
# MAGIC ```
# MAGIC %run ./00_config_utils
# MAGIC ```

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# 1. Load config.json
# ---------------------------------------------------------------------------
# config.json must live in the same folder as the notebooks.
# On Databricks, the working directory for %run notebooks is the repo/folder
# root, so a relative path of just "config.json" resolves correctly.
# If you placed it elsewhere, change the path below.

_CONFIG_PATH = "/Workspace/Users/praveenm191004@gmail.com/Capstone_Project/config.json"  # adjust to your workspace path

with open(_CONFIG_PATH, "r") as _f:
    _CFG = json.load(_f)

# ---------------------------------------------------------------------------
# 2. ADLS connection settings
# ---------------------------------------------------------------------------

_ADLS_CFG        = _CFG["adls"]
STORAGE_ACCOUNT  = _ADLS_CFG["storage_account_name"]
CONTAINER        = _ADLS_CFG["container_name"]
ACCOUNT_KEY      = _ADLS_CFG["account_key"]

# ADLS_OPTS is injected into every spark.read / df.write call via .options(**ADLS_OPTS).
ADLS_OPTS = {
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net": ACCOUNT_KEY,
}

# === THE CRITICAL FIX FOR SCD2 INCREMENTAL MERGES ===
# Globally register ADLS credentials for DeltaTable APIs
try:
    for key, value in ADLS_OPTS.items():
        spark.conf.set(key, value)
except Exception as e:
    print(f"Note: Could not set global spark.conf (safe to ignore if not running in Spark): {e}")

# Base abfss root for all data
BASE_DATA_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/capstone_project"


# ---------------------------------------------------------------------------
# 3. Pipeline execution parameters (Orchestrator Aware)
# ---------------------------------------------------------------------------

_PIPELINE_CFG    = _CFG["pipeline"]
_SEED_CFG        = _CFG["seed_sizes"]

# Helper to safely read Orchestrator parameters (which arrive as widgets) 
# without crashing if the notebook is run manually.
def get_runtime_arg(arg_name, default_val):
    try:
        val = dbutils.widgets.get(arg_name)
        return val if val else default_val
    except Exception:
        return default_val

# Try to get parameters from Orchestrator first, fallback to config/defaults
RUN_DATE         = get_runtime_arg("run_date", _PIPELINE_CFG.get("run_date", "").strip() or datetime.utcnow().strftime("%Y-%m-%d"))
GENERATION_MODE  = get_runtime_arg("generation_mode", _PIPELINE_CFG.get("generation_mode", "initial_seed").strip())
PIPELINE_RUN_ID  = get_runtime_arg("pipeline_run_id", str(uuid.uuid4()))

DIRTY_RATIO      = float(_PIPELINE_CFG.get("dirty_data_ratio", 0.06))
RANDOM_SEED      = int(_PIPELINE_CFG.get("random_seed", 42))

print(f"Storage account  : {STORAGE_ACCOUNT}")
print(f"Container        : {CONTAINER}")
print(f"Base data path   : {BASE_DATA_PATH}")
print(f"Run date         : {RUN_DATE}")
print(f"Generation mode  : {GENERATION_MODE}")
print(f"Pipeline Run Id  : {PIPELINE_RUN_ID}")

# ---------------------------------------------------------------------------
# 4. Path helpers  (no dbutils.fs)
# ---------------------------------------------------------------------------

def get_layer_path(layer: str, dataset: str = None) -> str:
    """
    Returns the abfss:// path for a given layer (raw / bronze / silver / gold / audit),
    optionally scoped to one dataset folder.
    """
    path = f"{BASE_DATA_PATH}/{layer}"
    if dataset:
        path = f"{path}/{dataset}"
    return path


def utc_now() -> datetime:
    return datetime.now(timezone.utc)

# ---------------------------------------------------------------------------
# 5. Audit columns
# ---------------------------------------------------------------------------

def add_bronze_audit_columns(df, run_id: str = None):
    run_id = run_id or PIPELINE_RUN_ID
    return (
        df.withColumn("_AdfPipelineRunId", F.lit(run_id).cast("string"))
          .withColumn("_IngestionTimestamp", F.current_timestamp())
    )


def add_silver_audit_columns(df, run_id: str = None):
    run_id = run_id or PIPELINE_RUN_ID
    return (
        df.withColumn("_AdfPipelineRunId", F.lit(run_id).cast("string"))
          .withColumn("_ProcessedTimestamp", F.current_timestamp())
    )

# ---------------------------------------------------------------------------
# 6. External Delta helpers  (path-based, no Unity Catalog, no saveAsTable)
# ---------------------------------------------------------------------------

def _table_path(schema_name: str, table_name: str) -> str:
    """Maps schema + table name to its abfss:// External Delta path."""
    return f"{BASE_DATA_PATH}/{schema_name}/{table_name}"


def write_delta_table(df, schema_name: str, table_name: str, mode: str = "append",
                       partition_by=None, merge_schema: bool = True) -> str:
    """
    Writes a DataFrame as an External Delta table directly to ADLS.
    Authentication is injected per-operation via ADLS_OPTS — no global spark.conf.set().
    Returns the abfss:// path written to.
    """
    path = _table_path(schema_name, table_name)
    writer = (
        df.write
          .format("delta")
          .mode(mode)
          .options(**ADLS_OPTS)
    )
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    return path


def read_delta_table(schema_name: str, table_name: str):
    """Reads an External Delta table from its abfss:// path."""
    path = _table_path(schema_name, table_name)
    return (
        spark.read
             .format("delta")
             .options(**ADLS_OPTS)
             .load(path)
    )


def table_exists(schema_name: str, table_name: str) -> bool:
    """
    Checks whether an External Delta table exists at the expected abfss:// path
    by attempting to read its _delta_log. Returns False if the path is absent or
    the folder contains no Delta log.
    """
    path = _table_path(schema_name, table_name)
    try:
        # .head() forces an immediate action over Spark Connect, ensuring 
        # the exception is thrown here if the path does not exist.
        spark.read.format("delta").options(**ADLS_OPTS).load(path).head()
        return True
    except Exception:
        return False

# ---------------------------------------------------------------------------
# 7. list_raw_files — uses binaryFile instead of dbutils.fs.ls
# ---------------------------------------------------------------------------

def list_raw_files(dataset: str):
    """
    Returns a list of lightweight namedtuple-like Row objects for every file
    currently in raw/<dataset> on ADLS, ignoring Spark marker files (_*).

    Uses spark.read.format("binaryFile") so authentication flows through
    ADLS_OPTS — dbutils.fs.ls is never called.

    Each returned Row has: .path  .name  .size  .modificationTime
    """
    path = get_layer_path("raw", dataset)
    try:
        files_df = (
            spark.read
                 .format("binaryFile")
                 .option("recursiveFileLookup", "true")
                 .options(**ADLS_OPTS)
                 .load(path)
                 # binaryFile schema: path, modificationTime, length, content
                 .select(
                     F.col("path"),
                     F.regexp_extract(F.col("path"), r"/([^/]+)$", 1).alias("name"),
                     F.col("length").alias("size"),
                     F.col("modificationTime"),
                 )
                 .filter(~F.col("name").startswith("_"))
        )
        return files_df.collect()  # list of Row(path, name, size, modificationTime)
    except Exception:
        return []

# ---------------------------------------------------------------------------
# 8. Audit / monitoring helpers
# ---------------------------------------------------------------------------

def log_pipeline_event(layer: str, dataset: str, status: str, records_in: int = None,
                        records_out: int = None, records_rejected: int = None,
                        error_message: str = None, run_id: str = None) -> None:
    """Appends one row to audit/pipeline_execution_log as an External Delta table."""
    run_id = run_id or PIPELINE_RUN_ID
    row = [(
        run_id, layer, dataset, status,
        int(records_in)       if records_in       is not None else None,
        int(records_out)      if records_out       is not None else None,
        int(records_rejected) if records_rejected  is not None else None,
        (error_message[:2000] if error_message else None),
    )]
    schema = T.StructType([
        T.StructField("pipeline_run_id",  T.StringType()),
        T.StructField("layer",            T.StringType()),
        T.StructField("dataset",          T.StringType()),
        T.StructField("status",           T.StringType()),
        T.StructField("records_in",       T.LongType()),
        T.StructField("records_out",      T.LongType()),
        T.StructField("records_rejected", T.LongType()),
        T.StructField("error_message",    T.StringType()),
    ])
    log_df = (
        spark.createDataFrame(row, schema)
             .withColumn("event_timestamp", F.current_timestamp())
    )
    write_delta_table(log_df, "audit", "pipeline_execution_log", mode="append")
    flag = "OK" if status == "SUCCESS" else ("..." if status == "STARTED" else "FAILED")
    print(
        f"[{flag}] {layer}.{dataset} -> {status}"
        + (f" | in={records_in} out={records_out} rejected={records_rejected}" if status == "SUCCESS" else "")
        + (f" | {error_message}" if error_message else "")
    )


def get_already_ingested_files(dataset: str) -> set:
    if not table_exists("audit", "bronze_file_registry"):
        return set()
    reg = (
        read_delta_table("audit", "bronze_file_registry")
        .filter(F.col("dataset") == dataset)
    )
    return {r["file_path"] for r in reg.select("file_path").collect()}


def register_ingested_files(dataset: str, file_rows, run_id: str = None) -> None:
    """
    file_rows — list of Row objects returned by list_raw_files()
    (fields: path, name, size, modificationTime).
    """
    if not file_rows:
        return
    run_id = run_id or PIPELINE_RUN_ID
    rows = [(dataset, r.path, r.name, int(r.size), run_id) for r in file_rows]
    schema = T.StructType([
        T.StructField("dataset",         T.StringType()),
        T.StructField("file_path",       T.StringType()),
        T.StructField("file_name",       T.StringType()),
        T.StructField("file_size",       T.LongType()),
        T.StructField("pipeline_run_id", T.StringType()),
    ])
    reg_df = (
        spark.createDataFrame(rows, schema)
             .withColumn("ingested_at", F.current_timestamp())
    )
    write_delta_table(reg_df, "audit", "bronze_file_registry", mode="append")


def get_silver_watermark(dataset: str):
    """
    Retrieves the latest watermark for a given dataset.
    Updated to look for the 'watermark' column instead of 'last_processed_ts'.
    """
    path = _table_path("audit", "silver_watermark")
    
    if not table_exists("audit", "silver_watermark"):
        return None
        
    try:
        # Read the table (with ADLS_OPTS safely injected)
        watermark_df = spark.read.format("delta").options(**ADLS_OPTS).load(path)
        
        # Filter for the specific dataset
        row = watermark_df.filter(F.col("dataset") == dataset).first()
        
        if row:
            # Retrieve the correct column name!
            return row["watermark"]
        return None
        
    except Exception as e:
        print(f"Warning: Could not retrieve watermark for {dataset}: {e}")
        return None


def set_silver_watermark(dataset: str, watermark_val):
    """
    Updates the watermark table without using raw spark.sql() 
    to bypass Databricks CE security restrictions on external storage.
    """
    path = _table_path("audit", "silver_watermark")
    
    # 1. Create a DataFrame for the single new row
    schema = T.StructType([
        T.StructField("dataset", T.StringType(), True),
        T.StructField("watermark", T.TimestampType(), True)
    ])
    new_row_df = spark.createDataFrame([(dataset, watermark_val)], schema)
    
    if not table_exists("audit", "silver_watermark"):
        # If table doesn't exist yet, simply append the first record
        new_row_df.write.format("delta").options(**ADLS_OPTS).mode("append").save(path)
    else:
        # 2. Read the existing table (Password injected safely!)
        existing_df = spark.read.format("delta").options(**ADLS_OPTS).load(path)
        
        # 3. Filter OUT the old record for this dataset (This acts as our DELETE)
        filtered_df = existing_df.filter(F.col("dataset") != dataset)
        
        # 4. Combine the remaining records with our new updated record
        final_df = filtered_df.union(new_row_df)
        
        # 5. Overwrite the table completely (Password injected safely!)
        (final_df.write
                 .format("delta")
                 .options(**ADLS_OPTS)
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .save(path))
# ---------------------------------------------------------------------------
# 9. Dataset registry  (unchanged business logic, referential_checks use paths)
# ---------------------------------------------------------------------------

DATASET_REGISTRY = {
    "products": {
        "raw_format": "csv",
        "bronze_schema": None,
        "is_json": False,
        "needs_flatten": False,
        "load_pattern": "overwrite_latest",
        "is_scd2": False,
        "primary_keys": ["ProductID"],
        "dedupe_keys": ["ProductID"],
        "not_null_columns": ["ProductID", "ProductName"],
        "dq_rules": [
            "CostPrice IS NOT NULL AND CostPrice <= 0",
        ],
        "referential_checks": [],
        "date_columns": {},
        "titlecase_columns": ["Category"],
        "silver_schema": {
            "ProductID":   "int",
            "ProductName": "string",
            "Category":    "string",
            "SubCategory": "string",
            "Brand":       "string",
            "CostPrice":   "decimal(10,2)",
        },
    },
    "customers": {
        "raw_format": "csv",
        "bronze_schema": {
            "CustomerID":  "int",
            "FirstName":   "string",
            "LastName":    "string",
            "Email":       "string",
            "Phone":       "string",
            "City":        "string",
            "State":       "string",
            "LastUpdated": "timestamp",
        },
        "is_json": False,
        "needs_flatten": False,
        "load_pattern": "append",
        "is_scd2": True,
        "scd2_business_key": "CustomerID",
        "scd2_tracked_columns": ["FirstName", "LastName", "Email", "Phone", "City", "State"],
        "watermark_column": "LastUpdated",
        "primary_keys": ["CustomerID"],
        "dedupe_keys": ["CustomerID", "LastUpdated"],
        "not_null_columns": ["CustomerID", "FirstName", "LastName"],
        "dq_rules": [],
        "referential_checks": [],
        "date_columns": {},
        "uppercase_columns": ["State"],
        "silver_schema": {
            "CustomerID":  "int",
            "FirstName":   "string",
            "LastName":    "string",
            "Email":       "string",
            "Phone":       "string",
            "City":        "string",
            "State":       "string",
            "LastUpdated": "timestamp",
        },
    },
    "exchange_rates": {
        "raw_format": "json",
        "bronze_schema": None,
        "is_json": True,
        "needs_flatten": True,
        "json_flatten_rename_map": {},
        "load_pattern": "append",
        "is_scd2": False,
        "primary_keys": ["BaseCurrency", "TargetCurrency", "RateDate"],
        "dedupe_keys": ["BaseCurrency", "TargetCurrency", "RateDate"],
        "not_null_columns": ["BaseCurrency", "TargetCurrency", "ExchangeRate", "RateDate"],
        "dq_rules": [
            "ExchangeRate IS NOT NULL AND ExchangeRate <= 0",
        ],
        "referential_checks": [],
        "date_columns": {"RateDate": "yyyy-MM-dd"},
        "silver_schema": {
            "BaseCurrency":  "string",
            "TargetCurrency":"string",
            "ExchangeRate":  "decimal(12,6)",
            "RateDate":      "date",
        },
    },
    "orders": {
        "raw_format": "csv",
        "bronze_schema": None,
        "is_json": False,
        "needs_flatten": False,
        "load_pattern": "append",
        "is_scd2": False,
        "primary_keys": ["OrderID"],
        "dedupe_keys": ["OrderID"],
        "not_null_columns": ["OrderID", "CustomerID", "ProductID", "OrderDate", "Quantity", "UnitPrice"],
        "dq_rules": [
            "Quantity IS NOT NULL AND Quantity <= 0",
            "UnitPrice IS NOT NULL AND UnitPrice <= 0",
        ],
        "referential_checks": [
            {"column": "CustomerID", "ref_schema": "silver", "ref_table": "customers", "ref_column": "CustomerID"},
            {"column": "ProductID",  "ref_schema": "silver", "ref_table": "products",  "ref_column": "ProductID"},
        ],
        "date_columns": {"OrderDate": "yyyy-MM-dd"},
        "silver_schema": {
            "OrderID":    "bigint",
            "CustomerID": "int",
            "ProductID":  "int",
            "OrderDate":  "date",
            "Quantity":   "int",
            "UnitPrice":  "decimal(10,2)",
            "StoreCode":  "string",
        },
    },
}

print(f"Registered datasets: {list(DATASET_REGISTRY.keys())}")