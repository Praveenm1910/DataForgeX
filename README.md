# Enterprise Retail Analytics Platform on Azure

## Project Overview

This project is an end-to-end **Data Engineering Capstone Project** demonstrating a production-grade **Medallion Architecture** (Raw $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold) built on Azure. It leverages **PySpark** and **Delta Lake** to process retail data, manage data quality, handle Slowly Changing Dimensions (SCD Type 2), and publish a star schema for business intelligence and reporting.

The platform is designed to be highly scalable, using **Azure Data Lake Storage Gen2 (ADLS Gen2)** for external Delta tables and a metadata-driven configuration approach.

---

## Architecture & Pipeline Components

The project is structured into sequentially executed Databricks notebooks, orchestrated by a central controller.

| Step | Notebook               | Description |
|------|------------------------|-------------|
| 0    | `00_config_utils`      | Central utility module. Loads `config.json`, manages ADLS authentication, defines the `DATASET_REGISTRY`, and handles custom path-based Delta operations. Loaded by all child notebooks via `%run`. |
| 1    | `01_data_generator`    | Generates synthetic raw landing-zone data (CSV/JSON files for Customers, Products, Orders, and Exchange Rates) mimicking real-world API and DB extracts. |
| 2    | `02_bronze_layer`      | Lands raw files into External Delta Bronze tables. Adds auditing metadata (`_SourceFile`, `_IngestionTimestamp`, `_AdfPipelineRunId`). |
| 3    | `03_silver_layer`      | The transformation engine. Cleans, casts types, flattens JSON, enforces data quality rules, and quarantines bad records into `{dataset}_rejected_records`. Applies **SCD Type 2** logic for Customers and schema evolution dynamically. |
| 4    | `04_gold_layer`        | Builds the presentation layer. Creates a dimensional Star Schema (`dim_customer`, `dim_product`, `dim_date`, `fact_sales`) utilizing complex point-in-time joins for currency exchange rates. Also builds business aggregates. |
| 5    | `05_orchestrator`      | The master entry point. Chains and executes the pipeline end-to-end based on the `orchestration_mode` (e.g., `initial_seed`, `daily_incremental`). |
| 6    | `06_publish_to_sql`    | Exports Gold presentation tables to Azure SQL via JDBC for downstream BI reporting tools. |
| 7    | `07_save_to_catalog`   | Registers Gold Delta tables directly into the Databricks Catalog (`capstone_gold_check`) for local SQL querying. |
| 8    | `08_test_cases`        | Executes data quality validations and unit tests against pipeline outputs. |

---

## Key Features & Technical Highlights

* **Metadata-Driven Processing:** Transformation logic, schemas, and Data Quality (DQ) rules are centrally managed via a `DATASET_REGISTRY` rather than hardcoded in notebooks.
* **Slowly Changing Dimensions (SCD2):** Implements robust historical tracking for Customer data using `MERGE INTO` operations via Delta Lake APIs.
* **Point-in-Time Fact Joins:** Uses advanced PySpark Window functions to accurately join orders with the most recent historical currency exchange rate *on or before* the transaction date.
* **Data Quality & Quarantining:** Enforces strict DQ rules. Records failing validation are automatically flagged (`_IsRejected = true`) and routed to separate reject tables for auditing without halting the pipeline.
* **Schema Evolution:** Natively supports and tracks structural changes using Delta Lake's `ALTER TABLE` and `mergeSchema` capabilities.
* **External Delta Tables:** Completely bypasses the local metastore for primary storage, writing directly to `abfss://` paths on ADLS Gen2 for decoupling compute and storage.

---

## Getting Started

### 1. Prerequisites

* An **Azure Databricks** Workspace.
* An **Azure Data Lake Storage Gen2 (ADLS Gen2)** account.
* An **Azure SQL Database** (optional, if using the `06_publish_to_sql` step).

### 2. Configuration

Before running the pipeline, update the `config.json` file located in the project root with your infrastructure details:

```json
{
  "azure_sql": {
    "server_name": "your-sql-server-name.database.windows.net",
    "database_name": "your_database_name",
    "username": "your_admin_user",
    "password": "your_secure_password",
    "tables_to_publish": [
      "dim_customer",
      "dim_product",
      "dim_date",
      "fact_sales",
      "agg_daily_sales_by_store",
      "agg_sales_by_category"
    ]
  },
  "pipeline": {
    "run_date": "",
    "generation_mode": "initial_seed",
    "orchestration_mode": "initial_seed",
    "dirty_data_ratio": 0.06,
    "random_seed": 42,
    "generate_mock_fx": true,
    "publish_to_sql": false,
    "save_to_catalog": true,
    "run_test_cases": true,
    "write_customers_to_sql": false
  },
  "seed_sizes": {
    "num_customers_seed": 1000,
    "num_new_customers_incremental": 150,
    "num_customers_to_update": 250,
    "num_products": 1500,
    "num_new_products_incremental": 500,
    "num_orders": 2500
  }
}
