# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold Layer  (ADLS Gen2 Edition)
# MAGIC **Enterprise Retail Analytics Platform on Azure**
# MAGIC
# MAGIC Builds a star schema on top of Silver. Every `spark.read` / `df.write` injects
# MAGIC `.options(**ADLS_OPTS)` for per-operation ADLS authentication.
# MAGIC No `dbutils.widgets`, no Unity Catalog, no `spark.conf.set()`.
# MAGIC
# MAGIC | Table | Type |
# MAGIC |-------|------|
# MAGIC | `gold/dim_customer`              | SCD2 dimension (surrogate key over Silver history) |
# MAGIC | `gold/dim_product`               | Type 1 dimension (latest snapshot) |
# MAGIC | `gold/dim_date`                  | Generated from observed Order date range |
# MAGIC | `gold/fact_sales`                | Fact table with point-in-time customer join |
# MAGIC | `gold/agg_daily_sales_by_store`  | Revenue/quantity by day + store |
# MAGIC | `gold/agg_sales_by_category`     | Revenue/quantity by product category |
# MAGIC
# MAGIC Run `03_silver_layer` first.

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------

from pyspark.sql.window import Window

# Gold object filter — edit directly or leave as "ALL" for automated runs.
# Valid values: "ALL", "dim_customer", "dim_product", "dim_date", "fact_sales", "aggregates"
OBJECT_FILTER = "ALL"


def should_build(name: str) -> bool:
    return OBJECT_FILTER in ("ALL", name)

# COMMAND ----------

# MAGIC %md ## ASSUMPTION: Store → Currency reference
# MAGIC Ten synthetic stores (`ST001`..`ST010`) mapped to currencies the exchange-rate feed
# MAGIC publishes. Replace with a real reference table when one is available.

# COMMAND ----------

STORE_CURRENCY_MAP = {
    "ST001": "USD", "ST002": "USD", "ST003": "EUR", "ST004": "GBP", "ST005": "INR",
    "ST006": "INR", "ST007": "JPY", "ST008": "CAD", "ST009": "AUD", "ST010": "CNY",
}
BASE_CURRENCY = "USD"

store_currency_df = spark.createDataFrame(
    [(k, v) for k, v in STORE_CURRENCY_MAP.items()],
    ["StoreCode", "LocalCurrency"],
)

# COMMAND ----------

# MAGIC %md ## `dim_customer` — SCD2 with surrogate key

# COMMAND ----------

if should_build("dim_customer"):
    log_pipeline_event("gold", "dim_customer", "STARTED")
    try:
        # read_delta_table injects ADLS_OPTS
        silver_customers = (
            read_delta_table("silver", "customers")
            .filter("_IsRejected = false")
        )
        window = Window.orderBy("CustomerID", "_SCD_EffectiveStartDate")
        dim_customer = (
            silver_customers
            .withColumn("CustomerSK", F.row_number().over(window))
            .select(
                "CustomerSK", "CustomerID", "FirstName", "LastName", "Email", "Phone",
                "City", "State", "_SCD_EffectiveStartDate", "_SCD_EffectiveEndDate", "_SCD_IsCurrent",
            )
        )
        row_count = dim_customer.count()
        # write_delta_table injects ADLS_OPTS
        write_delta_table(dim_customer, "gold", "dim_customer", mode="overwrite")
        log_pipeline_event("gold", "dim_customer", "SUCCESS", records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold", "dim_customer", "FAILED", error_message=str(e))
        raise

# COMMAND ----------

# MAGIC %md ## `dim_product` — Type 1 (latest snapshot)

# COMMAND ----------

if should_build("dim_product"):
    log_pipeline_event("gold", "dim_product", "STARTED")
    try:
        silver_products = (
            read_delta_table("silver", "products")
            .filter("_IsRejected = false")
        )
        product_window = Window.orderBy("ProductID")
        dim_product = (
            silver_products
            .dropDuplicates(["ProductID"])
            .withColumn("ProductSK", F.row_number().over(product_window))
            .select("ProductSK", "ProductID", "ProductName", "Category", "SubCategory", "Brand", "CostPrice")
        )
        row_count = dim_product.count()
        write_delta_table(dim_product, "gold", "dim_product", mode="overwrite")
        log_pipeline_event("gold", "dim_product", "SUCCESS", records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold", "dim_product", "FAILED", error_message=str(e))
        raise

# COMMAND ----------

# MAGIC %md ## `dim_date` — generated from the observed Order date range

# COMMAND ----------

if should_build("dim_date"):
    log_pipeline_event("gold", "dim_date", "STARTED")
    try:
        date_bounds = (
            read_delta_table("silver", "orders")
            .filter("_IsRejected = false")
            .agg(F.min("OrderDate").alias("min_d"), F.max("OrderDate").alias("max_d"))
            .first()
        )

        if date_bounds["min_d"] is None:
            log_pipeline_event("gold", "dim_date", "SUCCESS", records_out=0)
        else:
            import pandas as pd
            date_range = pd.date_range(
                start=date_bounds["min_d"],
                end=date_bounds["max_d"] + pd.Timedelta(days=1),
                freq="D",
            )
            date_rows = [
                (
                    int(d.strftime("%Y%m%d")), d.date().isoformat(),
                    int(d.year), int((d.month - 1) // 3 + 1),
                    int(d.month), d.strftime("%B"),
                    int(d.day), int(d.isoweekday()), d.strftime("%A"),
                    bool(d.isoweekday() in (6, 7)), int(d.isocalendar()[1]),
                )
                for d in date_range
            ]
            dim_date = spark.createDataFrame(date_rows, [
                "DateSK", "CalendarDate", "Year", "Quarter", "Month", "MonthName",
                "Day", "DayOfWeek", "DayName", "IsWeekend", "WeekOfYear",
            ])
            row_count = dim_date.count()
            write_delta_table(dim_date, "gold", "dim_date", mode="overwrite")
            log_pipeline_event("gold", "dim_date", "SUCCESS", records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold", "dim_date", "FAILED", error_message=str(e))
        raise

# COMMAND ----------

# MAGIC %md ## `fact_sales`
# MAGIC Point-in-time join to `dim_customer`: each order is matched to the SCD2 version of
# MAGIC the customer that was current **on the order date**, not the latest attributes today.

# COMMAND ----------

if should_build("fact_sales"):
    log_pipeline_event("gold", "fact_sales", "STARTED")
    try:
        # All reads inject ADLS_OPTS via read_delta_table
        orders       = read_delta_table("silver", "orders").filter("_IsRejected = false")
        dim_customer = read_delta_table("gold",   "dim_customer")
        dim_product  = read_delta_table("gold",   "dim_product")
        fx_rates     = read_delta_table("silver", "exchange_rates").filter("_IsRejected = false")

        orders_with_customer = orders.alias("o").join(
            dim_customer.alias("c"),
            (F.col("o.CustomerID") == F.col("c.CustomerID"))
            & (F.col("o.OrderDate") >= F.to_date("c._SCD_EffectiveStartDate"))
            & (
                F.col("c._SCD_EffectiveEndDate").isNull()
                | (F.col("o.OrderDate") < F.to_date("c._SCD_EffectiveEndDate"))
            ),
            "left",
        ).select("o.*", F.col("c.CustomerSK"))

        orders_with_product = orders_with_customer.join(
            dim_product.select("ProductID", "ProductSK", "CostPrice"),
            on="ProductID", how="left",
        )

        orders_with_currency = (
            orders_with_product
            .join(store_currency_df, on="StoreCode", how="left")
            .withColumn("LocalCurrency", F.coalesce(F.col("LocalCurrency"), F.lit(BASE_CURRENCY)))
        )

        # 1. Join orders to ALL exchange rates that occurred ON OR BEFORE the order date
        orders_with_fx = orders_with_currency.alias("o").join(
            fx_rates.alias("fx"),
            (F.col("o.LocalCurrency") == F.col("fx.TargetCurrency")) &
            (F.col("fx.BaseCurrency") == BASE_CURRENCY) &
            (F.col("fx.RateDate") <= F.col("o.OrderDate")),
            "left"
        )

        # 2. Rank the joined rates to find the MOST RECENT rate prior to the order
        # This brilliantly handles weekends (Saturday orders will match to Friday's rate!)
        window_fx = Window.partitionBy("o.OrderID").orderBy(F.col("fx.RateDate").desc())

        fact_sales = (
            orders_with_fx
            .withColumn("_fx_rank", F.row_number().over(window_fx))
            .filter("_fx_rank = 1") # Keep only the closest historical rate
            .withColumn("ExchangeRate",          F.coalesce(F.col("fx.ExchangeRate"), F.lit(1.0)))
            .withColumn("DateSK",                F.date_format("o.OrderDate", "yyyyMMdd").cast("int"))
            .withColumn("LineTotalLocal",        F.col("o.Quantity") * F.col("o.UnitPrice"))
            .withColumn("LineTotalBaseCurrency", F.round(F.col("LineTotalLocal") / F.col("ExchangeRate"), 2))
            .withColumn("LineCostBaseCurrency",  F.round((F.col("o.Quantity") * F.col("o.CostPrice")) / F.col("ExchangeRate"), 2))
            .withColumn("GrossMarginBaseCurrency", F.round(
                F.col("LineTotalBaseCurrency") - F.col("LineCostBaseCurrency"), 2
            ))
            .select(
                "o.OrderID", "DateSK", "o.CustomerSK", "o.ProductSK", "o.StoreCode", "o.LocalCurrency",
                "o.Quantity", "o.UnitPrice", "ExchangeRate",
                "LineTotalLocal", "LineTotalBaseCurrency", "LineCostBaseCurrency", "GrossMarginBaseCurrency",
            )
        )

        row_count = fact_sales.count()
        write_delta_table(fact_sales, "gold", "fact_sales", mode="overwrite")
        log_pipeline_event("gold", "fact_sales", "SUCCESS", records_out=row_count)
    except Exception as e:
        log_pipeline_event("gold", "fact_sales", "FAILED", error_message=str(e))
        raise

# COMMAND ----------

# MAGIC %md ## Business aggregates

# COMMAND ----------

if should_build("aggregates"):
    log_pipeline_event("gold", "aggregates", "STARTED")
    try:
        fact_sales  = read_delta_table("gold", "fact_sales")
        dim_date    = read_delta_table("gold", "dim_date")
        dim_product = read_delta_table("gold", "dim_product")

        daily_by_store = (
            fact_sales
            .join(dim_date, on="DateSK", how="left")
            .groupBy("CalendarDate", "StoreCode")
            .agg(
                F.countDistinct("OrderID").alias("TotalOrders"),
                F.sum("Quantity").alias("TotalQuantity"),
                F.sum("LineTotalBaseCurrency").alias(f"TotalRevenue{BASE_CURRENCY}"),
                F.sum("GrossMarginBaseCurrency").alias(f"TotalMargin{BASE_CURRENCY}"),
            )
        )
        write_delta_table(daily_by_store, "gold", "agg_daily_sales_by_store", mode="overwrite")

        by_category = (
            fact_sales
            .join(dim_product, on="ProductSK", how="left")
            .groupBy("Category", "SubCategory")
            .agg(
                F.sum("Quantity").alias("TotalQuantity"),
                F.sum("LineTotalBaseCurrency").alias(f"TotalRevenue{BASE_CURRENCY}"),
                F.sum("GrossMarginBaseCurrency").alias(f"TotalMargin{BASE_CURRENCY}"),
            )
        )
        write_delta_table(by_category, "gold", "agg_sales_by_category", mode="overwrite")

        log_pipeline_event(
            "gold", "aggregates", "SUCCESS",
            records_out=daily_by_store.count() + by_category.count(),
        )
    except Exception as e:
        log_pipeline_event("gold", "aggregates", "FAILED", error_message=str(e))
        raise

# COMMAND ----------

# MAGIC %md ## Sanity check

# COMMAND ----------

for tbl in ["dim_customer", "dim_product", "dim_date", "fact_sales",
            "agg_daily_sales_by_store", "agg_sales_by_category"]:
    if table_exists("gold", tbl):
        print(f"gold.{tbl}: {read_delta_table('gold', tbl).count()} rows")

if table_exists("gold", "fact_sales"):
    display(read_delta_table("gold", "fact_sales").limit(20))