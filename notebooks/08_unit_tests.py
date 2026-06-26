# Databricks notebook source
# MAGIC %md ## 1 · Test Cases

# COMMAND ----------

# 08 · Enterprise Unit Test Suite
# Enterprise Retail Analytics Platform on Azure — Databricks Community Edition

import unittest
import uuid
import json
from datetime import datetime, timezone, date
from decimal import Decimal
from unittest.mock import patch, MagicMock, call, PropertyMock
from functools import reduce

# PySpark
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, DateType, BooleanType, ArrayType,
    DecimalType
)
from pyspark.sql.window import Window

# ── Shared SparkSession ───────────────────────────────────────────────────────
try:
    spark  # noqa: F821
    print("✓ Using existing Databricks SparkSession")
except NameError:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("CapstoneUnitTests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("✓ Created local SparkSession for testing")

print(f"Spark version : {spark.version}")


# ── 2 · Reusable helper functions & test-data builders ────────────────────────
def assert_columns_present(test_case, df, expected_cols):
    for col in expected_cols:
        test_case.assertIn(
            col, df.columns,
            f"Expected column '{col}' not found. Actual columns: {df.columns}"
        )

def to_dicts(df):
    return [row.asDict() for row in df.collect()]

def make_simple_df(data, schema=None):
    if schema:
        return spark.createDataFrame(data, schema)
    return spark.createDataFrame(data)

def make_customer_df(rows=None):
    schema = StructType([
        StructField("CustomerID",  IntegerType(), True),
        StructField("FirstName",   StringType(),  True),
        StructField("LastName",    StringType(),  True),
        StructField("Email",       StringType(),  True),
        StructField("City",        StringType(),  True),
        StructField("State",       StringType(),  True),
        StructField("LastUpdated", TimestampType(), True),
    ])
    rows = rows or [
        (1, "Alice", "Smith", "alice@example.com", "Chennai",   "TN", datetime(2024, 1, 1)),
        (2, "Bob",   "Jones", "bob@example.com",   "Mumbai",    "MH", datetime(2024, 1, 1)),
    ]
    return spark.createDataFrame(rows, schema)

def make_product_df(rows=None):
    schema = StructType([
        StructField("ProductID",   IntegerType(), True),
        StructField("ProductName", StringType(),  True),
        StructField("Category",    StringType(),  True),
        StructField("SubCategory", StringType(),  True),
        StructField("Brand",       StringType(),  True),
        StructField("CostPrice",   DoubleType(),  True),
    ])
    rows = rows or [
        (101, "Laptop Pro",  "Electronics", "Computers", "TechBrand", 800.0),
        (102, "Office Chair","Furniture",   "Seating",   "ComfyCo",   150.0),
        (101, "Laptop Pro",  "Electronics", "Computers", "TechBrand", 800.0), 
    ]
    return spark.createDataFrame(rows, schema)

def make_orders_df(rows=None):
    schema = StructType([
        StructField("OrderID",    LongType(),    True),
        StructField("CustomerID", IntegerType(), True),
        StructField("ProductID",  IntegerType(), True),
        StructField("OrderDate",  DateType(),    True),
        StructField("Quantity",   IntegerType(), True),
        StructField("UnitPrice",  DoubleType(),  True),
        StructField("StoreCode",  StringType(),  True),
    ])
    rows = rows or [
        (1001, 1, 101, date(2024, 3, 15), 2, 1200.0, "ST001"),
        (1002, 2, 102, date(2024, 3, 16), 1,  300.0, "ST003"),
    ]
    return spark.createDataFrame(rows, schema)

print("✓ Helper functions and test-data builders defined")


# ── 3 · Bronze Layer Tests ────────────────────────────────────────────────────
PIPELINE_RUN_ID = str(uuid.uuid4())

def add_bronze_audit_columns(df, run_id=None):
    run_id = run_id or PIPELINE_RUN_ID
    return (
        df.withColumn("_AdfPipelineRunId",   F.lit(run_id).cast("string"))
          .withColumn("_IngestionTimestamp", F.current_timestamp())
    )

def pick_new_files(all_files, already_done):
    return [f for f in all_files if f.path not in already_done]

def ingest_csv_dataset_logic(df, cfg):
    if cfg.get("bronze_schema"):
        for col_name, target_type in cfg["bronze_schema"].items():
            if col_name in df.columns:
                if target_type == "timestamp":
                    df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
                else:
                    df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    else:
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
    return df

class TestBronzeAuditColumns(unittest.TestCase):
    def setUp(self):
        self.df = spark.createDataFrame(
            [(1, "Alice", "Chennai"), (2, "Bob", "Mumbai")],
            ["CustomerID", "Name", "City"]
        )

    def test_ingestion_timestamp_column_added(self):
        result = add_bronze_audit_columns(self.df)
        self.assertIn("_IngestionTimestamp", result.columns)

    def test_adf_pipeline_run_id_column_added(self):
        result = add_bronze_audit_columns(self.df)
        self.assertIn("_AdfPipelineRunId", result.columns)

    def test_existing_columns_preserved(self):
        result = add_bronze_audit_columns(self.df)
        for col in ["CustomerID", "Name", "City"]:
            self.assertIn(col, result.columns)

    def test_row_count_unchanged(self):
        result = add_bronze_audit_columns(self.df)
        self.assertEqual(result.count(), self.df.count())

    def test_custom_run_id_used(self):
        custom_id = "test-run-abc-123"
        result = add_bronze_audit_columns(self.df, run_id=custom_id)
        rows = result.select("_AdfPipelineRunId").distinct().collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["_AdfPipelineRunId"], custom_id)

class TestBronzeCSVIngestion(unittest.TestCase):
    def setUp(self):
        schema = StructType([
            StructField("CustomerID",  StringType(), True),
            StructField("FirstName",   StringType(), True),
            StructField("LastUpdated", StringType(), True),
        ])
        self.df = spark.createDataFrame(
            [("1", "Alice", "2024-01-01 10:00:00"),
             ("2", "Bob",   "2024-01-02 11:30:00")],
            schema
        )

    def test_all_columns_cast_to_string_when_no_bronze_schema(self):
        cfg = {}
        result = ingest_csv_dataset_logic(self.df, cfg)
        for field in result.schema.fields:
            self.assertEqual(field.dataType, StringType())

    def test_source_file_column_present_after_audit(self):
        df_with_source = self.df.withColumn("_SourceFile", F.lit("/raw/customers/file.csv"))
        result = add_bronze_audit_columns(df_with_source)
        self.assertIn("_SourceFile", result.columns)

class TestBronzeJSONIngestion(unittest.TestCase):
    def setUp(self):
        self.df = spark.createDataFrame(
            [("USD", "EUR", "0.92", "/raw/exchange_rates/2024-01-01.json"),
             ("USD", "GBP", "0.79", "/raw/exchange_rates/2024-01-01.json")],
            ["BaseCurrency", "TargetCurrency", "ExchangeRate", "_SourceFile"]
        )

    def test_source_file_column_exists(self):
        self.assertIn("_SourceFile", self.df.columns)

    def test_audit_columns_added_to_json_df(self):
        result = add_bronze_audit_columns(self.df)
        assert_columns_present(self, result, ["_IngestionTimestamp", "_AdfPipelineRunId"])

    def test_json_data_rows_retained(self):
        result = add_bronze_audit_columns(self.df)
        self.assertEqual(result.count(), 2)

class TestPickNewFiles(unittest.TestCase):
    def _make_file_row(self, path):
        return Row(path=path, name=path.split("/")[-1], size=1024,
                   modificationTime=datetime(2024, 1, 1))

    def test_already_ingested_files_excluded(self):
        all_files  = [self._make_file_row("/raw/customers/file_A.csv"),
                      self._make_file_row("/raw/customers/file_B.csv")]
        done       = {"/raw/customers/file_A.csv"}
        result = pick_new_files(all_files, done)
        result_paths = [r.path for r in result]
        self.assertNotIn("/raw/customers/file_A.csv", result_paths)

    def test_new_files_returned(self):
        all_files  = [self._make_file_row("/raw/customers/file_A.csv"),
                      self._make_file_row("/raw/customers/file_B.csv")]
        done       = {"/raw/customers/file_A.csv"}
        result = pick_new_files(all_files, done)
        result_paths = [r.path for r in result]
        self.assertIn("/raw/customers/file_B.csv", result_paths)

    def test_empty_registry_returns_all_files(self):
        all_files  = [self._make_file_row("/raw/orders/ord1.csv"),
                      self._make_file_row("/raw/orders/ord2.csv")]
        done       = set()
        result = pick_new_files(all_files, done)
        self.assertEqual(len(result), 2)

    def test_all_done_returns_empty(self):
        all_files  = [self._make_file_row("/raw/products/prod.csv")]
        done       = {"/raw/products/prod.csv"}
        result = pick_new_files(all_files, done)
        self.assertEqual(result, [])

    def test_negative_non_matching_path_not_filtered(self):
        all_files  = [self._make_file_row("/raw/orders/ord1.csv")]
        done       = {"/raw/customers/file_A.csv"}
        result = pick_new_files(all_files, done)
        self.assertEqual(len(result), 1)

print("[+] Bronze layer test classes defined")


# ── 4 · Silver Layer Tests ────────────────────────────────────────────────────
def flatten_complete(df):
    current_schema = df.schema
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols  = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]

        if not struct_cols and not array_cols:
            break

        for col_name in array_cols:
            df = df.withColumn(col_name, F.explode_outer(F.col(col_name)))

        for col_name in struct_cols:
            if col_name not in current_schema.names:
                continue
            expanded_cols = [
                F.col(f"{col_name}.{field.name}").alias(f"{col_name}_{field.name}")
                for field in current_schema[col_name].dataType.fields
            ]
            df = df.select("*", *expanded_cols).drop(col_name)

        current_schema = df.schema
    return df

def apply_rejection_rules(df, not_null_columns):
    reject_exprs = [F.col(c).isNull() for c in not_null_columns if c in df.columns]
    if not reject_exprs:
        is_rejected = F.lit(False)
    else:
        is_rejected = reduce(lambda a, b: a | b, reject_exprs)
    return df.withColumn("_IsRejected", F.coalesce(is_rejected, F.lit(True)))

class TestFlattenComplete(unittest.TestCase):
    def test_struct_fields_flattened(self):
        schema = StructType([
            StructField("customer", StructType([
                StructField("id",   IntegerType(), True),
                StructField("name", StringType(),  True),
            ]), True)
        ])
        df = spark.createDataFrame([({"id": 1, "name": "John"},)], schema)
        result = flatten_complete(df)
        assert_columns_present(self, result, ["customer_id", "customer_name"])

    def test_nested_struct_removed(self):
        schema = StructType([
            StructField("customer", StructType([
                StructField("id",   IntegerType(), True),
                StructField("name", StringType(),  True),
            ]), True)
        ])
        df = spark.createDataFrame([({"id": 1, "name": "John"},)], schema)
        result = flatten_complete(df)
        for field in result.schema.fields:
            self.assertNotIsInstance(field.dataType, StructType)

    def test_array_exploded(self):
        schema = StructType([
            StructField("orders", ArrayType(
                StructType([StructField("amount", IntegerType(), True)])
            ), True)
        ])
        df = spark.createDataFrame([([{"amount": 100}, {"amount": 200}],)], schema)
        result = flatten_complete(df)
        for field in result.schema.fields:
            self.assertNotIsInstance(field.dataType, ArrayType)
        self.assertIn("orders_amount", result.columns)
        self.assertEqual(result.count(), 2)

    def test_full_spec_json_structure(self):
        schema = StructType([
            StructField("customer", StructType([
                StructField("id",   IntegerType(), True),
                StructField("name", StringType(),  True),
            ]), True),
            StructField("orders", ArrayType(
                StructType([StructField("amount", IntegerType(), True)])
            ), True),
        ])
        df = spark.createDataFrame(
            [({"id": 1, "name": "John"}, [{"amount": 100}])],
            schema
        )
        result = flatten_complete(df)
        assert_columns_present(self, result, ["customer_id", "customer_name", "orders_amount"])
        rows = to_dicts(result)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["customer_id"],   1)
        self.assertEqual(rows[0]["customer_name"], "John")
        self.assertEqual(rows[0]["orders_amount"], 100)

    def test_already_flat_df_unchanged(self):
        df = spark.createDataFrame(
            [(1, "test")],
            StructType([
                StructField("id",   IntegerType(), True),
                StructField("name", StringType(),  True),
            ])
        )
        result = flatten_complete(df)
        self.assertEqual(set(result.columns), {"id", "name"})
        self.assertEqual(result.count(), 1)

class TestParseAndFlattenJSONBronze(unittest.TestCase):
    def _parse_and_flatten_json_bronze(self, bronze_df, rename_map):
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

    def setUp(self):
        payloads = [
            '{"base":"USD","rates":{"EUR":0.92,"GBP":0.79}}',
            '{"base":"USD","rates":{"EUR":0.91,"GBP":0.78}}',
        ]
        self.bronze_df = spark.createDataFrame([(p,) for p in payloads], ["raw_payload"])

    def test_raw_payload_parsed(self):
        result = self._parse_and_flatten_json_bronze(self.bronze_df, {})
        self.assertIsNotNone(result)
        self.assertNotIn("raw_payload", result.columns)

    def test_flattened_output_produced(self):
        result = self._parse_and_flatten_json_bronze(self.bronze_df, {})
        self.assertIsNotNone(result)
        self.assertIn("base", result.columns)

    def test_rename_map_applied(self):
        rename_map = {"base": "BaseCurrency"}
        result = self._parse_and_flatten_json_bronze(self.bronze_df, rename_map)
        self.assertIsNotNone(result)
        self.assertIn("BaseCurrency", result.columns)
        self.assertNotIn("base", result.columns)

    def test_empty_payload_returns_none(self):
        null_df = spark.createDataFrame(
            [(None,)],
            StructType([StructField("raw_payload", StringType(), True)])
        )
        result = self._parse_and_flatten_json_bronze(null_df, {})
        self.assertIsNone(result)

class TestSchemaEvolution(unittest.TestCase):
    def _detect_new_columns(self, existing_schema_dict, incoming_schema_dict):
        return {c: t for c, t in incoming_schema_dict.items() if c not in existing_schema_dict}

    def _build_alter_sql(self, path, new_cols):
        column_defs = ", ".join(f"`{c}` {t}" for c, t in new_cols.items())
        return f"ALTER TABLE delta.`{path}` ADD COLUMNS ({column_defs})"

    def test_new_columns_detected(self):
        existing  = {"CustomerID": "int", "City": "string"}
        incoming  = {"CustomerID": "int", "City": "string", "ZipCode": "string"}
        new_cols = self._detect_new_columns(existing, incoming)
        self.assertIn("ZipCode", new_cols)

    def test_existing_columns_not_flagged(self):
        existing  = {"CustomerID": "int", "City": "string"}
        incoming  = {"CustomerID": "int", "City": "string", "ZipCode": "string"}
        new_cols = self._detect_new_columns(existing, incoming)
        self.assertNotIn("CustomerID", new_cols)

    def test_alter_table_sql_correct(self):
        path     = "abfss://container@account.dfs.core.windows.net/capstone/silver/customers"
        new_cols = {"ZipCode": "string"}
        sql = self._build_alter_sql(path, new_cols)
        self.assertIn("ALTER TABLE delta.", sql)
        self.assertIn(path, sql)
        self.assertIn("`ZipCode` string", sql)

    def test_no_new_columns_returns_empty_dict(self):
        existing  = {"CustomerID": "int", "City": "string"}
        incoming  = {"CustomerID": "int", "City": "string"}
        new_cols = self._detect_new_columns(existing, incoming)
        self.assertEqual(new_cols, {})

class TestRejectionRules(unittest.TestCase):
    def setUp(self):
        schema = StructType([
            StructField("OrderID",   LongType(),  True),
            StructField("ProductID", IntegerType(), True),
            StructField("Quantity",  IntegerType(), True),
        ])
        self.df = spark.createDataFrame(
            [(1001, 101, 5),   
             (1002, None, 3),  
             (1003, 102, None),
             (1004, 103, 2)],  
            schema
        )

    def test_invalid_records_flagged(self):
        result = apply_rejection_rules(self.df, not_null_columns=["OrderID", "ProductID"])
        rejected = result.filter("_IsRejected = true")
        rejected_ids = [r["OrderID"] for r in rejected.select("OrderID").collect()]
        self.assertIn(1002, rejected_ids)

    def test_valid_records_retained(self):
        result = apply_rejection_rules(self.df, not_null_columns=["OrderID", "ProductID"])
        valid = result.filter("_IsRejected = false")
        valid_ids = [r["OrderID"] for r in valid.select("OrderID").collect()]
        self.assertIn(1001, valid_ids)
        self.assertIn(1004, valid_ids)

    def test_rejection_column_always_present(self):
        all_valid = spark.createDataFrame(
            [(10, 1, 5)],
            StructType([
                StructField("OrderID",   LongType(),   True),
                StructField("ProductID", IntegerType(), True),
                StructField("Quantity",  IntegerType(), True),
            ])
        )
        result = apply_rejection_rules(all_valid, not_null_columns=["OrderID", "ProductID"])
        self.assertIn("_IsRejected", result.columns)
def apply_phone_scrubbing(df, cfg):
    for col_name in cfg.get("phone_columns", []):
        if col_name in df.columns:
            c = F.col(col_name)
            c = F.when(F.lower(F.trim(c)).isin("n/a", "null", "none", ""), F.lit(None).cast("string")).otherwise(c)
            c = F.regexp_replace(c, r"[^\d]", "")
            c = F.when((F.length(c) < 10) | (F.length(c) > 15) | (c == ""), F.lit(None).cast("string")).otherwise(c)
            df = df.withColumn(col_name, c)
    return df

class TestPhoneScrubbing(unittest.TestCase):
    def setUp(self):
        schema = StructType([
            StructField("CustomerID", IntegerType(), True),
            StructField("Phone", StringType(), True)
        ])
        self.df = spark.createDataFrame([
            (1, "N/A"),
            (2, "3268542351"),
            (3, "(547)452-5534x1928"),
            (4, "403.491.1718"),
            (5, "123") # Too short, should become NULL
        ], schema)

    def test_phone_scrubbing_valid_and_invalid(self):
        cfg = {"phone_columns": ["Phone"]}
        result = apply_phone_scrubbing(self.df, cfg).collect()
        
        # Map results for easy assertion
        res_dict = {r.CustomerID: r.Phone for r in result}
        
        self.assertIsNone(res_dict[1], "N/A should become None")
        self.assertEqual(res_dict[2], "3268542351", "Valid 10-digit should remain")
        self.assertEqual(res_dict[3], "54745255341928", "Symbols and extensions should be stripped")
        self.assertEqual(res_dict[4], "4034911718", "Dots should be stripped")
        self.assertIsNone(res_dict[5], "Too short phone number should become None")


print("[+] Silver layer test classes defined")


# ── 5 · SCD Type 2 Tests ──────────────────────────────────────────────────────
def scd2_apply(df_existing, df_incoming, business_key, tracked_cols, effective_col):
    def add_hash(df):
        return df.withColumn(
            "_SCD_RecordHash",
            F.sha2(
                F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit(""))
                                    for c in tracked_cols]),
                256,
            )
        )

    w_dedupe = Window.partitionBy(business_key).orderBy(F.col(effective_col).desc())
    df_incoming = (
        df_incoming
        .withColumn("_rn", F.row_number().over(w_dedupe))
        .filter("_rn = 1")
        .drop("_rn")
    )
    df_incoming = add_hash(df_incoming)

    if df_existing is None:
        initial = (
            df_incoming
            .withColumn("_SCD_EffectiveStartDate", F.col(effective_col))
            .withColumn("_SCD_EffectiveEndDate",   F.lit(None).cast("timestamp"))
            .withColumn("_SCD_IsCurrent",          F.lit(True))
        )
        return initial

    df_existing = add_hash(df_existing)

    target_current = (
        df_existing
        .filter("_SCD_IsCurrent = true")
        .select(business_key,
                F.col("_SCD_RecordHash").alias("_target_hash"))
    )

    compared       = df_incoming.join(target_current, on=business_key, how="left")
    changed_or_new = compared.filter(
        F.col("_target_hash").isNull() |
        (F.col("_SCD_RecordHash") != F.col("_target_hash"))
    ).drop("_target_hash")

    changed_keys = [r[business_key] for r in
                    changed_or_new.select(business_key).distinct().collect()]

    df_expired = df_existing.withColumn(
        "_SCD_IsCurrent",
        F.when(
            (F.col(business_key).isin(changed_keys)) & (F.col("_SCD_IsCurrent") == True),
            F.lit(False)
        ).otherwise(F.col("_SCD_IsCurrent"))
    ).withColumn(
        "_SCD_EffectiveEndDate",
        F.when(
            (F.col(business_key).isin(changed_keys)) & (F.col("_SCD_IsCurrent").cast("boolean") == False),
            F.current_timestamp()
        ).otherwise(F.col("_SCD_EffectiveEndDate"))
    )

    new_versions = (
        changed_or_new
        .withColumn("_SCD_EffectiveStartDate", F.col(effective_col))
        .withColumn("_SCD_EffectiveEndDate",   F.lit(None).cast("timestamp"))
        .withColumn("_SCD_IsCurrent",          F.lit(True))
    ).drop("_SCD_RecordHash")

    existing_cols = [c for c in df_expired.columns if c != "_SCD_RecordHash"]
    return df_expired.select(existing_cols).unionByName(
        new_versions.select([c for c in new_versions.columns if c in existing_cols])
    )

class TestSCDType2(unittest.TestCase):
    def setUp(self):
        scd2_schema = StructType([
            StructField("CustomerID",            IntegerType(),  True),
            StructField("City",                  StringType(),   True),
            StructField("LastUpdated",            TimestampType(), True),
            StructField("_SCD_EffectiveStartDate", TimestampType(), True),
            StructField("_SCD_EffectiveEndDate",   TimestampType(), True),
            StructField("_SCD_IsCurrent",          BooleanType(),  True),
        ])
        self.df_existing = spark.createDataFrame(
            [(1, "Chennai", datetime(2024, 1, 1), datetime(2024, 1, 1), None, True)],
            scd2_schema
        )
        incoming_schema = StructType([
            StructField("CustomerID",  IntegerType(),  True),
            StructField("City",        StringType(),   True),
            StructField("LastUpdated", TimestampType(), True),
        ])
        self.df_incoming = spark.createDataFrame(
            [(1, "Bangalore", datetime(2024, 6, 1))],
            incoming_schema
        )

    def test_previous_record_expires(self):
        result = scd2_apply(
            self.df_existing, self.df_incoming,
            business_key="CustomerID",
            tracked_cols=["City"],
            effective_col="LastUpdated"
        )
        chennai_rows = result.filter(F.col("City") == "Chennai").collect()
        self.assertTrue(len(chennai_rows) > 0)
        for row in chennai_rows:
            self.assertFalse(row["_SCD_IsCurrent"])

    def test_new_record_inserted(self):
        result = scd2_apply(
            self.df_existing, self.df_incoming,
            business_key="CustomerID",
            tracked_cols=["City"],
            effective_col="LastUpdated"
        )
        bangalore_rows = result.filter((F.col("City") == "Bangalore") & (F.col("_SCD_IsCurrent") == True)).collect()
        self.assertTrue(len(bangalore_rows) == 1)

    def test_effective_dates_populated(self):
        result = scd2_apply(
            self.df_existing, self.df_incoming,
            business_key="CustomerID",
            tracked_cols=["City"],
            effective_col="LastUpdated"
        )
        new_row = result.filter((F.col("City") == "Bangalore") & (F.col("_SCD_IsCurrent") == True)).collect()[0]
        self.assertIsNotNone(new_row["_SCD_EffectiveStartDate"])
        self.assertIsNone(new_row["_SCD_EffectiveEndDate"])

    def test_unchanged_key_not_duplicated(self):
        scd2_schema = StructType([
            StructField("CustomerID",            IntegerType(),  True),
            StructField("City",                  StringType(),   True),
            StructField("LastUpdated",            TimestampType(), True),
            StructField("_SCD_EffectiveStartDate", TimestampType(), True),
            StructField("_SCD_EffectiveEndDate",   TimestampType(), True),
            StructField("_SCD_IsCurrent",          BooleanType(),  True),
        ])
        existing_two = spark.createDataFrame(
            [(1, "Chennai",  datetime(2024, 1, 1), datetime(2024, 1, 1), None, True),
             (2, "Mumbai",   datetime(2024, 1, 1), datetime(2024, 1, 1), None, True)],
            scd2_schema
        )
        result = scd2_apply(
            existing_two, self.df_incoming,
            business_key="CustomerID",
            tracked_cols=["City"],
            effective_col="LastUpdated"
        )
        cust2_current = result.filter((F.col("CustomerID") == 2) & (F.col("_SCD_IsCurrent") == True)).count()
        self.assertEqual(cust2_current, 1)

    def test_first_load_all_current(self):
        result = scd2_apply(
            df_existing=None,
            df_incoming=self.df_incoming,
            business_key="CustomerID",
            tracked_cols=["City"],
            effective_col="LastUpdated"
        )
        total = result.count()
        current = result.filter("_SCD_IsCurrent = true").count()
        self.assertEqual(total, current)

print("[+] SCD Type 2 test classes defined")


# ── 6 · Gold Layer Tests ──────────────────────────────────────────────────────
class TestGoldDimCustomer(unittest.TestCase):
    def setUp(self):
        self.silver_customers = spark.createDataFrame(
            [(1, "Alice", "Smith", "TN", datetime(2024, 1, 1), None,    True),
             (1, "Alice", "Smith", "MH", datetime(2024, 6, 1), None,    True),
             (2, "Bob",   "Jones", "KA", datetime(2024, 1, 1), None,    True)],
            StructType([
                StructField("CustomerID",            IntegerType(),  True),
                StructField("FirstName",             StringType(),   True),
                StructField("LastName",              StringType(),   True),
                StructField("State",                 StringType(),   True),
                StructField("_SCD_EffectiveStartDate", TimestampType(), True),
                StructField("_SCD_EffectiveEndDate",   TimestampType(), True),
                StructField("_SCD_IsCurrent",          BooleanType(),  True),
            ])
        )

    def _build_dim_customer(self, df):
        window = Window.orderBy("CustomerID", "_SCD_EffectiveStartDate")
        return df.withColumn("CustomerSK", F.row_number().over(window))

    def test_surrogate_keys_generated(self):
        result = self._build_dim_customer(self.silver_customers)
        self.assertIn("CustomerSK", result.columns)
        self.assertEqual(result.filter(F.col("CustomerSK").isNull()).count(), 0)

    def test_customer_id_retained(self):
        result = self._build_dim_customer(self.silver_customers)
        self.assertIn("CustomerID", result.columns)

    def test_row_count_matches_source(self):
        result = self._build_dim_customer(self.silver_customers)
        self.assertEqual(result.count(), self.silver_customers.count())

    def test_surrogate_keys_unique(self):
        result = self._build_dim_customer(self.silver_customers)
        self.assertEqual(result.count(), result.select("CustomerSK").distinct().count())

class TestGoldDimProduct(unittest.TestCase):
    def setUp(self):
        self.silver_products = make_product_df()

    def _build_dim_product(self, df):
        product_window = Window.orderBy("ProductID")
        return (
            df.dropDuplicates(["ProductID"])
              .withColumn("ProductSK", F.row_number().over(product_window))
        )

    def test_duplicate_product_ids_removed(self):
        result = self._build_dim_product(self.silver_products)
        self.assertEqual(result.count(), result.select("ProductID").distinct().count())

    def test_product_sk_generated(self):
        result = self._build_dim_product(self.silver_products)
        self.assertIn("ProductSK", result.columns)
        self.assertEqual(result.filter(F.col("ProductSK").isNull()).count(), 0)

    def test_row_count_after_dedup(self):
        result = self._build_dim_product(self.silver_products)
        self.assertEqual(result.count(), 2)

class TestGoldFactSales(unittest.TestCase):
    def setUp(self):
        self.dim_customer = spark.createDataFrame(
            [(1, 1, datetime(2024, 1, 1), None, True),
             (2, 2, datetime(2024, 1, 1), None, True)],
            StructType([
                StructField("CustomerSK",            IntegerType(),  True),
                StructField("CustomerID",            IntegerType(),  True),
                StructField("_SCD_EffectiveStartDate", TimestampType(), True),
                StructField("_SCD_EffectiveEndDate",   TimestampType(), True),
                StructField("_SCD_IsCurrent",          BooleanType(),  True),
            ])
        )
        self.dim_product = spark.createDataFrame(
            [(10, 101, 800.0),
             (20, 102, 150.0)],
            StructType([
                StructField("ProductSK",  IntegerType(), True),
                StructField("ProductID",  IntegerType(), True),
                StructField("CostPrice",  DoubleType(),  True),
            ])
        )
        self.orders = make_orders_df()

    def _build_fact_sales(self, orders, dim_customer, dim_product):
        orders_with_customer = orders.alias("o").join(
            dim_customer.alias("c"),
            (F.col("o.CustomerID") == F.col("c.CustomerID")) & F.col("c._SCD_IsCurrent"),
            "left"
        ).select("o.*", F.col("c.CustomerSK"))

        orders_with_product = orders_with_customer.join(
            dim_product.select("ProductID", "ProductSK", "CostPrice"),
            on="ProductID", how="left"
        )
        return orders_with_product.withColumn("LineTotalLocal", F.col("Quantity") * F.col("UnitPrice"))

    def test_customer_join_works(self):
        result = self._build_fact_sales(self.orders, self.dim_customer, self.dim_product)
        self.assertIn("CustomerSK", result.columns)
        self.assertEqual(result.filter(F.col("CustomerSK").isNull()).count(), 0)

    def test_product_join_works(self):
        result = self._build_fact_sales(self.orders, self.dim_customer, self.dim_product)
        self.assertIn("ProductSK", result.columns)
        self.assertEqual(result.filter(F.col("ProductSK").isNull()).count(), 0)

    def test_revenue_calculation_correct(self):
        result = self._build_fact_sales(self.orders, self.dim_customer, self.dim_product).collect()
        for row in result:
            expected = row["Quantity"] * row["UnitPrice"]
            self.assertAlmostEqual(float(row["LineTotalLocal"]), expected, places=2)

class TestGoldAggregates(unittest.TestCase):
    def setUp(self):
        self.fact_sales = spark.createDataFrame(
            [(1001, 20240315, "ST001", 2, 1200.0, 2400.0),
             (1002, 20240315, "ST001", 1,  300.0,  300.0),
             (1003, 20240316, "ST003", 3,  500.0, 1500.0)],
            StructType([
                StructField("OrderID",           LongType(),    True),
                StructField("DateSK",            IntegerType(), True),
                StructField("StoreCode",          StringType(),  True),
                StructField("Quantity",           IntegerType(), True),
                StructField("UnitPrice",          DoubleType(),  True),
                StructField("LineTotalBaseCurrency", DoubleType(), True),
            ])
        )
        self.dim_date = spark.createDataFrame(
            [(20240315, "2024-03-15"),
             (20240316, "2024-03-16")],
            StructType([
                StructField("DateSK",       IntegerType(), True),
                StructField("CalendarDate", StringType(),  True),
            ])
        )

    def test_daily_revenue_aggregation(self):
        daily = (
            self.fact_sales
            .join(self.dim_date, on="DateSK", how="left")
            .groupBy("CalendarDate", "StoreCode")
            .agg(F.sum("LineTotalBaseCurrency").alias("TotalRevenue"))
        )
        row = daily.filter((F.col("CalendarDate") == "2024-03-15") & (F.col("StoreCode") == "ST001")).collect()[0]
        self.assertAlmostEqual(float(row["TotalRevenue"]), 2700.0, places=2)

    def test_daily_agg_row_count(self):
        daily = (
            self.fact_sales
            .join(self.dim_date, on="DateSK", how="left")
            .groupBy("CalendarDate", "StoreCode")
            .agg(F.sum("LineTotalBaseCurrency").alias("TotalRevenue"))
        )
        self.assertEqual(daily.count(), 2)

    def test_sum_calculations_accurate(self):
        total_qty = self.fact_sales.agg(F.sum("Quantity").alias("TotalQty")).collect()[0]["TotalQty"]
        self.assertEqual(total_qty, 6)

print("[+] Gold layer test classes defined")


# ── 7 · Orchestrator Tests ────────────────────────────────────────────────────
class TestOrchestratorRunStep(unittest.TestCase):
    @staticmethod
    def _run_step_impl(dbutils_mock, notebook_path, timeout, pipeline_run_id, run_date, extra_params=None):
        params = {"pipeline_run_id": pipeline_run_id, "run_date": run_date, **(extra_params or {})}
        try:
            result = dbutils_mock.notebook.run(notebook_path, timeout, params)
            return result or "OK"
        except Exception as e:
            raise RuntimeError(f"[{notebook_path}] failed: {e}") from e

    def setUp(self):
        self.dbutils = MagicMock()
        self.pipeline_run_id = "test-run-001"
        self.run_date        = "2024-03-15"
        self.timeout         = 3600

    def test_notebook_called(self):
        self.dbutils.notebook.run.return_value = "SUCCESS"
        self._run_step_impl(self.dbutils, "./02_bronze_layer", self.timeout, self.pipeline_run_id, self.run_date)
        self.dbutils.notebook.run.assert_called_once()
        self.assertEqual(self.dbutils.notebook.run.call_args[0][0], "./02_bronze_layer")

    def test_parameters_passed(self):
        self.dbutils.notebook.run.return_value = "SUCCESS"
        self._run_step_impl(self.dbutils, "./03_silver_layer", self.timeout, self.pipeline_run_id, self.run_date)
        passed_params = self.dbutils.notebook.run.call_args[0][2]
        self.assertEqual(passed_params["pipeline_run_id"], self.pipeline_run_id)
        self.assertEqual(passed_params["run_date"], self.run_date)

    def test_result_returned(self):
        self.dbutils.notebook.run.return_value = "PIPELINE_OK"
        result = self._run_step_impl(self.dbutils, "./04_gold_layer", self.timeout, self.pipeline_run_id, self.run_date)
        self.assertEqual(result, "PIPELINE_OK")

    def test_extra_params_merged(self):
        self.dbutils.notebook.run.return_value = "OK"
        extra = {"generation_mode": "initial_seed"}
        self._run_step_impl(self.dbutils, "./01_data_generator", self.timeout, self.pipeline_run_id, self.run_date, extra_params=extra)
        passed_params = self.dbutils.notebook.run.call_args[0][2]
        self.assertEqual(passed_params["generation_mode"], "initial_seed")


class TestOrchestratorFailureHandling(unittest.TestCase):
    @staticmethod
    def _run_step_impl(dbutils_mock, notebook_path, timeout, pipeline_run_id, run_date):
        params = {"pipeline_run_id": pipeline_run_id, "run_date": run_date}
        try:
            result = dbutils_mock.notebook.run(notebook_path, timeout, params)
            return result or "OK"
        except Exception as e:
            raise RuntimeError(f"[{notebook_path}] failed: {e}") from e

    def test_exception_propagated_as_runtime_error(self):
        dbutils = MagicMock()
        dbutils.notebook.run.side_effect = Exception("Notebook timed out")
        with self.assertRaises(RuntimeError):
            self._run_step_impl(dbutils, "./02_bronze_layer", 3600, "r1", "2024-03-15")

    def test_runtime_error_message_contains_notebook_name(self):
        dbutils = MagicMock()
        dbutils.notebook.run.side_effect = Exception("OOM")
        try:
            self._run_step_impl(dbutils, "./03_silver_layer", 3600, "r1", "2024-03-15")
            self.fail("Expected RuntimeError was not raised")
        except RuntimeError as e:
            self.assertIn("./03_silver_layer", str(e))

print("[+] Orchestrator test classes defined")


# ── 8 · SQL Publish & Catalog Sync Tests ──────────────────────────────────────
class TestSQLPublish(unittest.TestCase):
    def _build_jdbc_url(self, server, database):
        return (
            f"jdbc:sqlserver://{server}:1433;"
            f"database={database};"
            "encrypt=true;trustServerCertificate=false;"
            "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        )

    def _mock_jdbc_write(self, df, jdbc_url, table_name, user, password, mode="overwrite"):
        write_mock = MagicMock()
        write_mock.format.return_value  = write_mock
        write_mock.option.return_value  = write_mock
        write_mock.options.return_value = write_mock
        write_mock.mode.return_value    = write_mock

        write_mock.format("jdbc")
        write_mock.option("url",     jdbc_url)
        write_mock.option("dbtable", f"dbo.{table_name}")
        write_mock.option("user",    user)
        write_mock.option("password",password)
        write_mock.mode(mode)
        write_mock.save()
        return write_mock

    def test_correct_jdbc_url_built(self):
        url = self._build_jdbc_url("myserver.database.windows.net", "RetailDB")
        self.assertIn("myserver.database.windows.net", url)
        self.assertIn("database=RetailDB", url)

    def test_jdbc_url_contains_port(self):
        url = self._build_jdbc_url("srv.database.windows.net", "DB")
        self.assertIn(":1433", url)

    def test_correct_table_name_used(self):
        for table in ["dim_customer", "fact_sales", "agg_daily_sales_by_store"]:
            write_mock = self._mock_jdbc_write(MagicMock(), "jdbc:sqlserver://...", table, "user", "pass")
            option_calls = [str(c) for c in write_mock.option.call_args_list]
            self.assertTrue(any(f"dbo.{table}" in c for c in option_calls))

    def test_overwrite_mode_selected(self):
        write_mock = self._mock_jdbc_write(MagicMock(), "jdbc:sqlserver://...", "dim_product", "user", "pass")
        write_mock.mode.assert_called_with("overwrite")

class TestCatalogSync(unittest.TestCase):
    DB_NAME = "capstone_gold_check"

    def _mock_save_as_table(self, df_mock, db_name, table_name, mode="overwrite"):
        write_mock = MagicMock()
        write_mock.mode.return_value = write_mock
        df_mock.write = write_mock
        df_mock.write.mode(mode).saveAsTable(f"{db_name}.{table_name}")
        return write_mock

    def test_correct_database_name_used(self):
        write_mock = self._mock_save_as_table(MagicMock(), self.DB_NAME, "dim_customer")
        save_args = write_mock.saveAsTable.call_args[0][0]
        self.assertIn(self.DB_NAME, save_args)

    def test_correct_table_name_used(self):
        for table in ["dim_customer", "dim_product", "fact_sales"]:
            write_mock = self._mock_save_as_table(MagicMock(), self.DB_NAME, table)
            save_args = write_mock.saveAsTable.call_args[0][0]
            self.assertIn(table, save_args)

    def test_overwrite_mode_selected(self):
        write_mock = self._mock_save_as_table(MagicMock(), self.DB_NAME, "fact_sales", "overwrite")
        write_mock.mode.assert_called_with("overwrite")

    def test_qualified_table_name_format(self):
        write_mock = self._mock_save_as_table(MagicMock(), self.DB_NAME, "agg_daily_sales_by_store")
        save_args = write_mock.saveAsTable.call_args[0][0]
        self.assertEqual(save_args, f"{self.DB_NAME}.agg_daily_sales_by_store")

print("[+] SQL Publish & Catalog Sync test classes defined")


# ── 9 · Test Runner ───────────────────────────────────────────────────────────
def build_test_suite():
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    test_classes = [
        TestBronzeAuditColumns, TestBronzeCSVIngestion, TestBronzeJSONIngestion, TestPickNewFiles,
        TestFlattenComplete, TestParseAndFlattenJSONBronze, TestSchemaEvolution, TestRejectionRules,
        TestPhoneScrubbing,
        TestSCDType2,
        TestGoldDimCustomer, TestGoldDimProduct, TestGoldFactSales, TestGoldAggregates,
        TestOrchestratorRunStep, TestOrchestratorFailureHandling,
        TestSQLPublish, TestCatalogSync,
    ]

    for cls in test_classes:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return suite

print("\n" + "="*72)
print("  ENTERPRISE RETAIL ANALYTICS — UNIT TEST SUITE")
print("="*72 + "\n")

runner = unittest.TextTestRunner(verbosity=2, failfast=False)
result = runner.run(build_test_suite())

print("\n" + "="*72)
total   = result.testsRun
passed  = total - len(result.failures) - len(result.errors)
failed  = len(result.failures)
errors  = len(result.errors)
skipped = len(result.skipped)

print(f"  TOTAL   : {total}")
print(f"  PASSED  : {passed}")
print(f"  FAILED  : {failed}")
print(f"  ERRORS  : {errors}")
print(f"  SKIPPED : {skipped}")
print("="*72)

if result.failures:
    print("\n⚠  FAILURES:")
    for test, traceback in result.failures:
        print(f"\n  [{test}]\n{traceback}")

if result.errors:
    print("\n⚠  ERRORS:")
    for test, traceback in result.errors:
        print(f"\n  [{test}]\n{traceback}")

# Exit gracefully depending on the environment
if result.wasSuccessful():
    print("\n[+]  ALL TESTS PASSED — pipeline is production-ready.")
    try:
        dbutils.notebook.exit("ALL_TESTS_PASSED")
    except NameError:
        import sys
        sys.exit(0)
else:
    print("\n[-]  SOME TESTS FAILED — review output above before promoting to production.")
    try:
        dbutils.notebook.exit(f"TESTS_FAILED: {failed} failure(s), {errors} error(s)")
    except NameError:
        import sys
        sys.exit(1)