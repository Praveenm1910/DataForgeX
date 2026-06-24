# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Pipeline Orchestrator  (ADLS Gen2 Edition)
# MAGIC **Enterprise Retail Analytics Platform on Azure**
# MAGIC
# MAGIC Single-notebook entry point that chains all four layers end-to-end using
# MAGIC `dbutils.notebook.run()`.
# MAGIC
# MAGIC **No `dbutils.widgets`** — all execution parameters are read from `config.json`
# MAGIC (via the same `_CFG` object populated by `00_config_utils`).
# MAGIC
# MAGIC %md
# MAGIC | Step | Notebook             | What it does |
# MAGIC |------|----------------------|--------------|
# MAGIC | 0    | `00_config_utils`    | Loaded by each child notebook via `%run` |
# MAGIC | 1    | `01_data_generator`  | Generates raw CSV/JSON files for the run date |
# MAGIC | 2    | `02_bronze_layer`    | Lands raw files into External Delta Bronze tables |
# MAGIC | 3    | `03_silver_layer`    | Cleans, casts, validates, SCD2 → External Delta Silver |
# MAGIC | 4    | `04_gold_layer`      | Builds star schema + aggregates in External Delta Gold |
# MAGIC | 5    | `05_orchestrator`    | Master entry point that chains and executes the pipeline end-to-end |
# MAGIC | 6    | `06_publish_to_sql`  | Exports Gold presentation tables to Azure SQL via JDBC for reporting |
# MAGIC | 7    | `07_save_to_catalog` | Registers Gold Delta tables into Databricks Catalog (`capstone_gold_check`) |
# MAGIC | 8    | `08_test_cases`      | Executes data quality validations and unit tests against pipeline outputs |
# MAGIC
# MAGIC ### Orchestration modes  (set `orchestration_mode` in config.json)
# MAGIC * **`initial_seed`** — generates the first full dataset snapshot, then runs Bronze→Gold.
# MAGIC * **`daily_incremental`** — generates only the day's delta files, then runs Bronze→Gold.
# MAGIC * **`bronze_to_gold_only`** — skips `01_data_generator` entirely (ADF / real source lands files).

# COMMAND ----------

import json
import uuid
from datetime import datetime

# ---------------------------------------------------------------------------
# Load config.json directly (this notebook does not %run 00_config_utils
# because it does not need Spark or ADLS — it only drives dbutils.notebook.run).
# Adjust the path to match your workspace layout.
# ---------------------------------------------------------------------------

_CONFIG_PATH = "path to your config.json file"  # adjust to your workspace path

with open(_CONFIG_PATH, "r") as _f:
    _CFG = json.load(_f)

_PIPELINE_CFG      = _CFG["pipeline"]
_SEED_CFG          = _CFG["seed_sizes"]

ORCHESTRATION_MODE = _PIPELINE_CFG.get("orchestration_mode", "daily_incremental").strip()
RUN_DATE           = _PIPELINE_CFG.get("run_date", "").strip() or datetime.utcnow().strftime("%Y-%m-%d")
DIRTY_RATIO        = str(_PIPELINE_CFG.get("dirty_data_ratio", 0.06))
PIPELINE_RUN_ID    = str(uuid.uuid4())

# Per-notebook timeout in seconds — increase for very large seed runs.
TIMEOUT = 3600

print(f"=== Orchestrator starting ===")
print(f"mode            : {ORCHESTRATION_MODE}")
print(f"run_date        : {RUN_DATE}")
print(f"pipeline_run_id : {PIPELINE_RUN_ID}")
print(f"timeout/nb      : {TIMEOUT}s")

# COMMAND ----------

# MAGIC %md ## Helper: run a child notebook

# COMMAND ----------

def run_step(notebook_path: str, extra_params: dict = None, label: str = None) -> str:
    """
    Wraps `dbutils.notebook.run()` with timing and error propagation.
    Child notebooks receive the pipeline_run_id and run_date so all layers
    share the same lineage identifier.

    Note: child notebooks read their own `config.json` via `00_config_utils`,
    so only cross-cutting parameters need to be forwarded here.
    """
    label  = label or notebook_path.split("/")[-1]
    params = {
        "pipeline_run_id": PIPELINE_RUN_ID,
        "run_date":        RUN_DATE,
        **(extra_params or {}),
    }
    start = datetime.utcnow()
    print(f"\n→ [{label}] starting at {start.strftime('%H:%M:%S')} UTC")
    try:
        result  = dbutils.notebook.run(notebook_path, TIMEOUT, params)
        elapsed = (datetime.utcnow() - start).total_seconds()
        print(f"✓ [{label}] completed in {elapsed:.1f}s  result={result!r}")
        return result or "OK"
    except Exception as e:
        elapsed = (datetime.utcnow() - start).total_seconds()
        print(f"✗ [{label}] FAILED after {elapsed:.1f}s")
        raise RuntimeError(f"[{label}] failed: {e}") from e

# COMMAND ----------

# MAGIC %md ## Step 1 — Data Generator  (skipped in `bronze_to_gold_only` mode)

# COMMAND ----------

if ORCHESTRATION_MODE in ("initial_seed", "daily_incremental"):
    gen_mode = "initial_seed" if ORCHESTRATION_MODE == "initial_seed" else "daily_incremental"
    run_step(
        "./01_data_generator",
        extra_params={
            # The generator reads most settings from config.json itself,
            # but generation_mode needs to be explicit so the orchestrator can
            # override it without editing config.json on every run.
            "generation_mode": gen_mode,
        },
        label="01_data_generator",
    )
else:
    print(f"\n→ [01_data_generator] skipped (mode={ORCHESTRATION_MODE})")

# COMMAND ----------

# MAGIC %md ## Step 2 — Bronze

# COMMAND ----------

run_step(
    "./02_bronze_layer",
    label="02_bronze_layer",
)

# COMMAND ----------

# MAGIC %md ## Step 3 — Silver

# COMMAND ----------

run_step(
    "./03_silver_layer",
    label="03_silver_layer",
)

# COMMAND ----------

# MAGIC %md ## Step 4 — Gold

# COMMAND ----------

run_step(
    "./04_gold_layer",
    label="04_gold_layer",
)

# COMMAND ----------

# MAGIC %md ## Step 5 - Publish to Azure SQL

# COMMAND ----------

# Check the config toggle before attempting to run the SQL export
PUBLISH_TO_SQL = _PIPELINE_CFG.get("publish_to_sql", False)

if PUBLISH_TO_SQL:
    run_step(
        "./06_publish_to_sql",
        label="06_publish_to_sql",
    )
else:
    print("\n→ [06_publish_to_sql] skipped (publish_to_sql is false in config)")

# COMMAND ----------

# MAGIC %md ## Step 6 - Publish to Databricks

# COMMAND ----------

# Check the config toggle before attempting to run the Catalog Sync
SAVE_TO_CATALOG = _PIPELINE_CFG.get("save_to_catalog", False)

if SAVE_TO_CATALOG:
    run_step(
        "./07_save_to_catalog",
        label="07_save_to_catalog",
    )
else:
    print("\n→ [07_save_to_catalog] skipped (save_to_catalog is false in config)")

# COMMAND ----------

# MAGIC %md ## Done

# COMMAND ----------

summary = (
    f"Pipeline complete | run_date={RUN_DATE} | mode={ORCHESTRATION_MODE} "
    f"| pipeline_run_id={PIPELINE_RUN_ID}"
)
print(f"\n=== {summary} ===")
dbutils.notebook.exit(summary)