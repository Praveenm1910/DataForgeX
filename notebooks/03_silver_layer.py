# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver Layer  (ADLS Gen2 Edition)
# MAGIC **Enterprise Retail Analytics Platform on Azure**
# MAGIC
# MAGIC Generic, registry-driven Silver transformation. Key architectural changes from the
# MAGIC original:
# MAGIC
# MAGIC * **No Unity Catalog** — schema evolution uses `ALTER TABLE delta.\`<abfss://path>\``
# MAGIC   instead of registered table names.
# MAGIC * **SCD2** uses `DeltaTable.forPath(spark, path)` instead of `DeltaTable.forName()`.
# MAGIC * **Every `spark.read` / `df.write`** injects `.options(**ADLS_OPTS)`.
# MAGIC * **No `dbutils.widgets`** — all config comes from `config.json` via `00_config_utils`.
# MAGIC
# MAGIC Run `02_bronze_layer` first.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from functools import reduce
from delta.tables import DeltaTable
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.window import Window

# Dataset filter — edit directly or leave as "ALL" for automated runs.
DATASET_FILTER = "ALL"

datasets_to_process = (
    DATASET_REGISTRY if DATASET_FILTER == "ALL"
    else {DATASET_FILTER: DATASET_REGISTRY[DATASET_FILTER]}
)
print(f"Processing: {list(datasets_to_process.keys())}")

# COMMAND ----------

# MAGIC %md ## JSON flatten

# COMMAND ----------

def flatten_complete(df):
    """
    Generic recursive flattener: explodes arrays, expands structs, repeats
    until the DataFrame is fully tabular.
    """
    current_schema = df.schema
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not struct_cols and not array_cols:
            break

        for col_name in array_cols:
            df = df.withColumn(col_name, explode_outer(col(col_name)))

        for col_name in struct_cols:
            if col_name not in current_schema.names:
                continue
            expanded_cols = [
                col(f"{col_name}.{field.name}").alias(f"{col_name}_{field.name}")
                for field in current_schema[col_name].dataType.fields
            ]
            df = df.select("*", *expanded_cols).drop(col_name)

        current_schema = df.schema

    return df


def parse_and_flatten_json_bronze(bronze_df, rename_map: dict):
    """Re-parses Bronze's raw_payload STRING column back into structured JSON."""
    sample_row = bronze_df.filter(F.col("raw_payload").isNotNull()).first()
    if not sample_row:
        return None

    json_schema = spark.range(1).select(
        F.schema_of_json(F.lit(sample_row["raw_payload"]))
    ).first()[0]

    parsed = bronze_df.select(
        F.from_json("raw_payload", json_schema).alias("data")
    ).select("data.*")

    flat = flatten_complete(parsed)
    for old_name, new_name in rename_map.items():
        if old_name in flat.columns:
            flat = flat.withColumnRenamed(old_name, new_name)
    return flat

# COMMAND ----------

# MAGIC %md ## Schema evolution
# MAGIC Uses `ALTER TABLE delta.\`<abfss://path>\`` — no registered table name required,
# MAGIC fully compatible with Databricks Serverless and ADLS Gen2 External Delta tables.

# COMMAND ----------

def evolve_schema_and_write(df, schema_name: str, table_name: str, mode: str = "append") -> str:
    """
    Diffs the incoming DataFrame schema against the existing External Delta table,
    issues `ALTER TABLE delta.\`<path>\` ADD COLUMNS (...)` for any new columns,
    then writes the data.

    Authentication for the write goes through ADLS_OPTS inside write_delta_table().
    The ALTER TABLE statement operates on the Delta log via the abfss:// path so
    no Unity Catalog registration is needed.
    """
    path = _table_path(schema_name, table_name)  # abfss:// path from 00_config_utils

    if not table_exists(schema_name, table_name):
        write_delta_table(df, schema_name, table_name, mode="overwrite")
        return path

    # Introspect the existing External Delta table schema directly from Delta
    existing_df   = spark.read.format("delta").options(**ADLS_OPTS).load(path)
    table_schema  = {f.name: f.dataType.simpleString() for f in existing_df.schema.fields}
    incoming_schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    new_cols      = {c: t for c, t in incoming_schema.items() if c not in table_schema}

    if new_cols:
        column_defs = ", ".join(f"`{c}` {t}" for c, t in new_cols.items())
        # Path-based ALTER TABLE — works on Serverless without a registered table name
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({column_defs})")
        print(f"  [schema evolution] {path} += {list(new_cols.keys())}")

    # write_delta_table injects ADLS_OPTS
    write_delta_table(df, schema_name, table_name, mode=mode, merge_schema=True)
    return path

# COMMAND ----------

# MAGIC %md ## Clean, cast, validate

# COMMAND ----------

def clean_and_cast(df, cfg: dict):
    before = df.count()
    df = df.dropDuplicates(cfg["dedupe_keys"]) if cfg["dedupe_keys"] else df.dropDuplicates()
    after_dedupe = df.count()

    for col_name, target_type in cfg["silver_schema"].items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(target_type))
            continue
        if col_name in cfg["date_columns"]:
            df = df.withColumn(col_name, F.to_date(F.trim(F.col(col_name)), cfg["date_columns"][col_name]))
        elif target_type == "timestamp":
            df = df.withColumn(col_name, F.to_timestamp(F.trim(F.col(col_name))))
        elif target_type.startswith("decimal") or target_type in ("int", "bigint", "double", "float"):
            cleaned = F.regexp_replace(F.trim(F.col(col_name)), r"[^0-9.\-]", "")
            cleaned = F.when(cleaned == "", None).otherwise(cleaned)
            df = df.withColumn(col_name, cleaned.cast(target_type))
        else:
            df = df.withColumn(col_name, F.trim(F.col(col_name)).cast(target_type))

    for col_name in cfg.get("uppercase_columns", []):
        if col_name in df.columns:
            df = df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))
    for col_name in cfg.get("titlecase_columns", []):
        if col_name in df.columns:
            df = df.withColumn(col_name, F.initcap(F.trim(F.col(col_name))))

    reject_exprs   = [F.col(c).isNull() for c in cfg["not_null_columns"] if c in df.columns]
    reject_exprs  += [F.expr(rule) for rule in cfg.get("dq_rules", [])]

    aliases_to_drop = []
    for check in cfg.get("referential_checks", []):
        col_name, ref_schema, ref_table, ref_col = (
            check["column"], check["ref_schema"], check["ref_table"], check["ref_column"]
        )
        if col_name not in df.columns or not table_exists(ref_schema, ref_table):
            continue
        ref_alias = f"_ref_{col_name}"
        # read_delta_table injects ADLS_OPTS
        ref_df    = (
            read_delta_table(ref_schema, ref_table)
            .select(F.col(ref_col).alias(ref_alias))
            .distinct()
        )
        df = df.join(ref_df, df[col_name] == ref_df[ref_alias], "left")
        reject_exprs.append(F.col(col_name).isNotNull() & F.col(ref_alias).isNull())
        aliases_to_drop.append(ref_alias)

    is_rejected = reduce(lambda a, b: a | b, reject_exprs) if reject_exprs else F.lit(False)
    df = df.withColumn("_IsRejected", F.coalesce(is_rejected, F.lit(True)))

    for alias in aliases_to_drop:
        df = df.drop(alias)

    return df, before, after_dedupe

# COMMAND ----------

# MAGIC %md ## SCD Type 2 (Customers)
# MAGIC Uses `DeltaTable.forPath(spark, path)` with the `abfss://` path — no Unity Catalog
# MAGIC table registration required.

# COMMAND ----------

def upsert_scd2(df_incoming, schema_name: str, table_name: str, cfg: dict, run_id: str = None):
    business_key  = cfg["scd2_business_key"]
    tracked_cols  = cfg["scd2_tracked_columns"]
    effective_col = cfg["watermark_column"]
    run_id        = run_id or PIPELINE_RUN_ID
    path          = _table_path(schema_name, table_name)  # abfss:// path

    # --- NEW FIX: Deduplicate incoming batch to guarantee ONE row per business key ---
    window_dedupe = Window.partitionBy(business_key).orderBy(F.col(effective_col).desc())
    df_incoming = (
        df_incoming
        .withColumn("_rn_dedupe", F.row_number().over(window_dedupe))
        .filter("_rn_dedupe = 1")
        .drop("_rn_dedupe")
    )
    # ---------------------------------------------------------------------------------

    df_incoming = df_incoming.withColumn(
        "_SCD_RecordHash",
        F.sha2(
            F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_cols]),
            256,
        ),
    )

    if not table_exists(schema_name, table_name):
        initial = (
            df_incoming
            .withColumn("_SCD_EffectiveStartDate", F.col(effective_col))
            .withColumn("_SCD_EffectiveEndDate",   F.lit(None).cast("timestamp"))
            .withColumn("_SCD_IsCurrent",          F.lit(True))
        )
        initial = add_silver_audit_columns(initial, run_id)
        write_delta_table(initial, schema_name, table_name, mode="overwrite")
        return path, initial.count(), 0

    # read_delta_table injects ADLS_OPTS
    target_current = (
        read_delta_table(schema_name, table_name)
        .filter("_SCD_IsCurrent = true")
        .select(business_key, F.col("_SCD_RecordHash").alias("_target_hash"))
    )

    compared        = df_incoming.join(target_current, on=business_key, how="left")
    changed_or_new  = compared.filter(
        F.col("_target_hash").isNull() | (F.col("_SCD_RecordHash") != F.col("_target_hash"))
    ).drop("_target_hash")

    changed_keys = [r[business_key] for r in changed_or_new.select(business_key).distinct().collect()]
    if not changed_keys:
        return path, 0, df_incoming.count()

    # DeltaTable.forPath — uses the abfss:// path directly, no registered table name needed.
    # Databricks Serverless resolves ADLS credentials via the cluster's storage account key
    # already set in ADLS_OPTS; forPath does not need an extra option dict here because the
    # Delta log access goes through the same Spark session that has the key in its conf via
    # the prior write operations in this session.
    # 1. Register the incoming new/changed rows as a temporary view
    # 1. Register the incoming new/changed rows as a temporary view
    changed_or_new.createOrReplaceTempView("scd2_source")

    # 2. Format the ADLS_OPTS dictionary into a SQL OPTIONS string
    options_sql = ", ".join([f"`{k}` '{v}'" for k, v in ADLS_OPTS.items()])

    # 3. Create an updatable Temp View over the target Delta table 
    # Notice we pass `path` INSIDE the OPTIONS block now, not as a LOCATION!
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW scd2_target
        USING delta
        OPTIONS (
            {options_sql},
            `path` '{path}'
        )
    """)

    # 4. Execute the SCD2 MERGE INTO via pure Spark SQL
    spark.sql(f"""
        MERGE INTO scd2_target AS t
        USING scd2_source AS s
        ON t.{business_key} = s.{business_key} AND t._SCD_IsCurrent = true
        WHEN MATCHED THEN UPDATE SET
            t._SCD_IsCurrent = false,
            t._SCD_EffectiveEndDate = s.{effective_col}
    """)

    new_versions = (
        changed_or_new
        .withColumn("_SCD_EffectiveStartDate", F.col(effective_col))
        .withColumn("_SCD_EffectiveEndDate",   F.lit(None).cast("timestamp"))
        .withColumn("_SCD_IsCurrent",          F.lit(True))
    )
    new_versions = add_silver_audit_columns(new_versions, run_id)
    # evolve_schema_and_write uses abfss:// path and ADLS_OPTS internally
    evolve_schema_and_write(new_versions, schema_name, table_name, mode="append")

    return path, len(changed_keys), df_incoming.count() - len(changed_keys)

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

failures = []

for dataset, cfg in datasets_to_process.items():
    log_pipeline_event("silver", dataset, "STARTED")
    try:
        if not table_exists("bronze", dataset):
            log_pipeline_event("silver", dataset, "SUCCESS", records_in=0, records_out=0)
            print(f"[skip] {dataset}: no Bronze table yet")
            continue

        # read_delta_table injects ADLS_OPTS
        bronze_df_full = read_delta_table("bronze", dataset)

        if cfg["load_pattern"] == "append":
            last_watermark = get_silver_watermark(dataset)
            bronze_df = (
                bronze_df_full.filter(F.col("_IngestionTimestamp") > F.lit(last_watermark))
                if last_watermark is not None else bronze_df_full
            )
        else:
            bronze_df = bronze_df_full

        records_in = bronze_df.count()
        if records_in == 0:
            log_pipeline_event("silver", dataset, "SUCCESS", records_in=0, records_out=0)
            print(f"[skip] {dataset}: no new Bronze rows since last Silver run")
            continue

        if cfg["is_json"] and cfg["needs_flatten"]:
            if "raw_payload" in bronze_df.columns:
                working_df = parse_and_flatten_json_bronze(bronze_df, cfg["json_flatten_rename_map"])
            else:
                # The data is already structured (like Frankfurter API), just flatten it!
                working_df = flatten_complete(bronze_df)
                # If it's exchange rates, pivot the columns into rows
                if dataset == "exchange_rates":
                    # Get all currency columns (everything except date and base)
                    currency_cols = [c for c in working_df.columns if c.startswith("rates_")]
                
                    # Stack the columns into rows AND explicitly cast all numbers to double
                    stack_expr = f"stack({len(currency_cols)}, " + ", ".join([f"'{c.replace('rates_', '')}', cast({c} as double)" for c in currency_cols]) + ") as (TargetCurrency, ExchangeRate)"
                
                    # FIX 1: Explicitly retain the Bronze audit columns during the select
                    working_df = working_df.select(
                        "_SourceFile", "_IngestionTimestamp", 
                        "date", "base", F.expr(stack_expr)
                    ).withColumnRenamed("date", "RateDate") \
                     .withColumnRenamed("base", "BaseCurrency")
            
            # Now we check if the result is valid
            if working_df is None:
                log_pipeline_event("silver", dataset, "SUCCESS", records_in=0, records_out=0)
                continue
        else:
            # Non-JSON or non-flattened datasets
            # FIX 2: Protect _SourceFile and _IngestionTimestamp from being dropped!
            keep_cols = [
                c for c in bronze_df.columns 
                if not c.startswith("_") or c in ["_SourceFile", "_IngestionTimestamp"]
            ]
            working_df = bronze_df.select(*keep_cols)

        clean_df, before, after_dedupe = clean_and_cast(working_df, cfg)
        print(f"  {dataset}: {before} -> {after_dedupe} rows after dedupe")

        good_df      = clean_df.filter("_IsRejected = false")
        rejected_df  = clean_df.filter("_IsRejected = true")
        rejected_count = rejected_df.count()

        if rejected_count > 0:
            rejected_audited = add_silver_audit_columns(rejected_df)
            evolve_schema_and_write(rejected_audited, "silver", f"{dataset}_rejected_records", mode="append")

        if cfg["is_scd2"]:
            path, changed_ct, unchanged_ct = upsert_scd2(good_df, "silver", dataset, cfg)
            records_out = changed_ct
            print(f"  {dataset}: SCD2 -> {changed_ct} new/changed version(s), {unchanged_ct} unchanged")
        else:
            good_audited = add_silver_audit_columns(good_df)
            write_mode   = "append" if cfg["load_pattern"] == "append" else "overwrite"
            evolve_schema_and_write(good_audited, "silver", dataset, mode=write_mode)
            records_out  = good_df.count()

        log_pipeline_event(
            "silver", dataset, "SUCCESS",
            records_in=records_in, records_out=records_out, records_rejected=rejected_count,
        )

        if cfg["load_pattern"] == "append":
            new_watermark = bronze_df.agg(F.max("_IngestionTimestamp")).first()[0]
            if new_watermark is not None:
                set_silver_watermark(dataset, new_watermark)

    except Exception as e:
        log_pipeline_event("silver", dataset, "FAILED", error_message=str(e))
        failures.append((dataset, str(e)))

# COMMAND ----------

if failures:
    summary = "; ".join(f"{d}: {e}" for d, e in failures)
    raise RuntimeError(f"Silver layer failed for {len(failures)} dataset(s): {summary}")
else:
    print("Silver layer completed successfully for all requested datasets.")

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

for dataset in datasets_to_process:
    if table_exists("silver", dataset):
        cnt = read_delta_table("silver", dataset).count()
        print(f"silver.{dataset}: {cnt} rows")
    if table_exists("silver", f"{dataset}_rejected_records"):
        rcnt = read_delta_table("silver", f"{dataset}_rejected_records").count()
        print(f"silver.{dataset}_rejected_records: {rcnt} rows")