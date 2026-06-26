# Databricks notebook source
import json
import uuid
from datetime import datetime

# ---------------------------------------------------------------------------
# 1. Load config & Runtime Arguments
# ---------------------------------------------------------------------------
_CONFIG_PATH = "your path to config.json file"  

with open(_CONFIG_PATH, "r") as _f:
    _CFG = json.load(_f)

# --- ADF OVERRIDE LOGIC ---
def parse_bool(val):
    return str(val).lower() in ("true", "1", "yes")

def override_config_from_adf(cfg_dict, section_name):
    for key, default_val in cfg_dict[section_name].items():
        try:
            dbutils.widgets.text(key, "")
            adf_val = dbutils.widgets.get(key).strip()
            if adf_val:
                if isinstance(default_val, bool):
                    cfg_dict[section_name][key] = parse_bool(adf_val)
                elif isinstance(default_val, int):
                    cfg_dict[section_name][key] = int(adf_val)
                elif isinstance(default_val, float):
                    cfg_dict[section_name][key] = float(adf_val)
                else:
                    cfg_dict[section_name][key] = adf_val
        except Exception:
            pass

# Apply ADF Overrides before doing anything else
override_config_from_adf(_CFG, "pipeline")
override_config_from_adf(_CFG, "seed_sizes")

_PIPELINE_CFG = _CFG["pipeline"]
_SEED_CFG     = _CFG["seed_sizes"]

# Get the pipeline run ID from ADF, or generate a fake one
try:
    dbutils.widgets.text("pipeline_run_id", "")
    PIPELINE_RUN_ID = dbutils.widgets.get("pipeline_run_id").strip() or str(uuid.uuid4())
except Exception:
    PIPELINE_RUN_ID = str(uuid.uuid4())

ORCHESTRATION_MODE = _PIPELINE_CFG.get("orchestration_mode", "daily_incremental").strip()
RUN_DATE           = _PIPELINE_CFG.get("run_date", "").strip() or datetime.utcnow().strftime("%Y-%m-%d")

TIMEOUT = 3600

print(f"=== Orchestrator starting ===")
print(f"mode            : {ORCHESTRATION_MODE}")
print(f"run_date        : {RUN_DATE}")
print(f"pipeline_run_id : {PIPELINE_RUN_ID}")
print(f"timeout/nb      : {TIMEOUT}s")

# ---------------------------------------------------------------------------
# 2. Execution Tracker & Child Notebook Forwarding
# ---------------------------------------------------------------------------
_execution_log = []

def print_summary():
    print("\n" + "="*50)
    print(" PIPELINE EXECUTION SUMMARY")
    print("="*50)
    for step in _execution_log:
        status_icon = "[+]" if step['status'] == "SUCCESS" else ("[->]" if step['status'] == "SKIPPED" else "[!]")
        print(f"{status_icon} {step['layer'].ljust(20)} | {step['status'].ljust(8)} | {step['duration']}s")
    print("="*50 + "\n")

# Prepare the forwarded parameters for child notebooks (must be strings)
FORWARDED_PARAMS = {k: str(v).lower() if isinstance(v, bool) else str(v) for k, v in _PIPELINE_CFG.items()}
FORWARDED_PARAMS["pipeline_run_id"] = PIPELINE_RUN_ID

def run_step(notebook_path: str, extra_params: dict = None, label: str = None) -> str:
    label  = label or notebook_path.split("/")[-1]
    
    # Merge the forwarded ADF parameters with any step-specific parameters
    params = {
        **FORWARDED_PARAMS,
        **(extra_params or {}),
    }
    
    start = datetime.utcnow()
    print(f"\n-> [{label}] starting at {start.strftime('%H:%M:%S')} UTC")
    
    try:
        result = dbutils.notebook.run(notebook_path, TIMEOUT, params)
        elapsed = round((datetime.utcnow() - start).total_seconds(), 1)
        print(f"[+] [{label}] completed in {elapsed}s")
        _execution_log.append({"layer": label, "status": "SUCCESS", "duration": elapsed})
        return result or "OK"
        
    except Exception as e:
        elapsed = round((datetime.utcnow() - start).total_seconds(), 1)
        _execution_log.append({"layer": label, "status": "FAILED", "duration": elapsed})
        print_summary()
        error_msg = f"PIPELINE HALTED AT: {label}. Check notebook run logs. Error: {str(e)}"
        print(f"[!] {error_msg}")
        raise Exception(error_msg)

def skip_step(label: str, reason: str):
    print(f"\n[->] [{label}] skipped ({reason})")
    _execution_log.append({"layer": label, "status": "SKIPPED", "duration": 0.0})

# ---------------------------------------------------------------------------
# 3. Pipeline Execution Steps
# ---------------------------------------------------------------------------

# Step 1 - Data Generator
if ORCHESTRATION_MODE in ("initial_seed", "daily_incremental"):
    gen_mode = "initial_seed" if ORCHESTRATION_MODE == "initial_seed" else "daily_incremental"
    run_step("./01_data_generator", extra_params={"generation_mode": gen_mode}, label="01_data_generator")
else:
    skip_step("01_data_generator", f"mode={ORCHESTRATION_MODE}")

# Step 2 - Bronze
run_step("./02_bronze_layer", label="02_bronze_layer")

# Step 3 - Silver
run_step("./03_silver_layer", label="03_silver_layer")

# Step 4 - Gold
run_step("./04_gold_layer", label="04_gold_layer")

# Step 5 - Publish to Azure SQL
if _PIPELINE_CFG.get("publish_to_sql", False):
    run_step("./06_publish_to_sql", label="06_publish_to_sql")
else:
    skip_step("06_publish_to_sql", "publish_to_sql is false")

# Step 6 - Publish to Databricks Catalog
if _PIPELINE_CFG.get("save_to_catalog", False):
    run_step("./07_save_to_catalog", label="07_save_to_catalog")
else:
    skip_step("07_save_to_catalog", "save_to_catalog is false")

# Step 7 - Unit Tests
if _PIPELINE_CFG.get("run_test_cases", False):
    run_step("./08_unit_tests", label="08_unit_tests")
else:
    skip_step("08_unit_tests", "run_test_cases is false")

# ---------------------------------------------------------------------------
# 4. Final Output
# ---------------------------------------------------------------------------
print_summary()
summary = f"Pipeline complete | run_date={RUN_DATE} | mode={ORCHESTRATION_MODE} | pipeline_run_id={PIPELINE_RUN_ID}"
dbutils.notebook.exit(summary)