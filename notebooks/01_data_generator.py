# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Raw Data Generator  (ADLS Gen2 Edition)
# MAGIC **Enterprise Retail Analytics Platform on Azure**
# MAGIC
# MAGIC Generates synthetic raw landing-zone files for all four source systems and writes them
# MAGIC **directly to ADLS Gen2** using Spark (`coalesce(1).write`).
# MAGIC
# MAGIC `dbutils.fs.put` and `dbutils.widgets` are **not used anywhere in this notebook**.
# MAGIC All parameters come from `config.json` via `00_config_utils`.
# MAGIC
# MAGIC Run this notebook **before** `02_bronze_layer`.

# COMMAND ----------

# MAGIC %pip install faker --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config_utils

# COMMAND ----------


import random
import json
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

# ---------------------------------------------------------------------------
# Parameters — all sourced from config.json (loaded in 00_config_utils)
# ---------------------------------------------------------------------------

RUN_DATE         = _CFG["pipeline"].get("run_date", "").strip() or datetime.utcnow().strftime("%Y-%m-%d")
GENERATION_MODE  = _CFG["pipeline"].get("generation_mode", "initial_seed").strip()
DIRTY_RATIO      = float(_CFG["pipeline"].get("dirty_data_ratio", 0.06))
WRITE_CUSTOMERS_TO_SQL = _CFG["pipeline"].get("write_customers_to_sql", False) # <-- NEW TOGGLE

_SEEDS           = _CFG["seed_sizes"]
NUM_CUSTOMERS_SEED      = int(_SEEDS.get("num_customers_seed", 500))
NUM_NEW_CUSTOMERS_INCR  = int(_SEEDS.get("num_new_customers_incremental", 15))
NUM_CUSTOMERS_TO_UPDATE = int(_SEEDS.get("num_customers_to_update", 25))
NUM_PRODUCTS     = int(_SEEDS.get("num_products", 150))
NUM_ORDERS       = int(_SEEDS.get("num_orders", 400))

random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)
fake = Faker()

RUN_TS = RUN_DATE.replace("-", "")  # YYYYMMDD — used in filenames
print(f"run_date={RUN_DATE}  mode={GENERATION_MODE}  dirty_ratio={DIRTY_RATIO}")
print(f"write_customers_to_sql={WRITE_CUSTOMERS_TO_SQL}")

# ---------------------------------------------------------------------------
# Reference data & helpers
# ---------------------------------------------------------------------------

CATEGORY_TREE = {
    "Electronics":   ["Smartphones", "Laptops", "Headphones", "Televisions", "Cameras"],
    "Apparel":       ["Mens Wear", "Womens Wear", "Footwear", "Accessories"],
    "Home & Kitchen":["Cookware", "Furniture", "Decor", "Appliances"],
    "Sports":        ["Fitness", "Outdoor", "Team Sports"],
    "Grocery":       ["Beverages", "Snacks", "Staples"],
}
BRANDS       = ["Zentra", "Northwind", "Bluepeak", "Veloria", "Crestline", "Pixelhive", "Marsh & Co", "Aurex"]
STORE_CODES  = [f"ST{n:03d}" for n in range(1, 11)]

def maybe(prob: float) -> bool:
    return random.random() < prob

# ---------------------------------------------------------------------------
# Azure SQL Helpers (For Dynamic Customer Routing)
# ---------------------------------------------------------------------------

def get_sql_options(table_name: str) -> dict:
    sql_cfg = _CFG.get("azure_sql", {})
    # Serverless requires explicit parameters instead of a single JDBC URL string
    return {
        "host": sql_cfg.get("server_name"),
        "port": "1433",
        "database": sql_cfg.get("database_name"),
        "dbtable": table_name,
        "user": sql_cfg.get("username"),
        "password": sql_cfg.get("password"),
        "encrypt": "true",
        "trustservercertificate": "false",
        "connectiontimeout": "60"
    }

def write_pandas_to_sql(pandas_df: pd.DataFrame, table_name: str, write_mode: str = "overwrite"):
    print(f"  -> Writing {len(pandas_df)} rows to Azure SQL table: {table_name} (mode: {write_mode})...")
    
    # Force ALL columns to be raw strings (exactly like saving to a CSV file)
    # This prevents Azure SQL from guessing data types and leaves casting to the Silver Layer
    raw_string_df = pandas_df.astype(str).replace("None", "").replace("nan", "")
    spark_df = spark.createDataFrame(raw_string_df)
    
    # CHANGED: "jdbc" -> "sqlserver" to support Databricks Serverless compute write restrictions
    spark_df.write.format("sqlserver").options(**get_sql_options(table_name)).mode(write_mode).save()
    print("  -> SQL write complete!")

def read_sql_to_pandas(table_name: str) -> pd.DataFrame:
    try:
        # We cast to string so it behaves exactly like reading a raw CSV file
        # CHANGED: "jdbc" -> "sqlserver"
        return spark.read.format("sqlserver").options(**get_sql_options(table_name)).load().toPandas().astype("string")
    except Exception as e:
        print(f"  -> Could not read SQL table {table_name} (it may not exist yet).")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# ADLS write/read helpers
# ---------------------------------------------------------------------------

def write_csv_to_raw(pandas_df: "pd.DataFrame", dataset: str, filename: str) -> str:
    target_path = f"{get_layer_path('raw', dataset)}/{filename}"
    csv_content = pandas_df.astype(str).replace("None", "").replace("nan", "")
    spark_df = spark.createDataFrame(csv_content)
    (
        spark_df
        .coalesce(1)
        .write
        .format("csv")
        .mode("overwrite")
        .options(**ADLS_OPTS)
        .option("header", "true")
        .save(target_path)
    )
    print(f"  wrote {len(pandas_df):>6} rows -> {target_path}")
    return target_path

def write_json_to_raw(payload: dict, dataset: str, filename: str) -> str:
    target_path = f"{get_layer_path('raw', dataset)}/{filename}"
    json_str     = json.dumps(payload, indent=2, default=str)
    single_row_df = spark.createDataFrame([(json_str,)], ["value"])
    (
        single_row_df
        .coalesce(1)
        .write
        .format("text")
        .mode("overwrite")
        .options(**ADLS_OPTS)
        .save(target_path)
    )
    print(f"  wrote payload -> {target_path}")
    return target_path

def read_existing_ids(dataset: str, id_column: str):
    # DYNAMIC TOGGLE: If it's customers and the flag is True, read from Azure SQL!
    if dataset == "customers" and WRITE_CUSTOMERS_TO_SQL:
        try:
            # CHANGED: "jdbc" -> "sqlserver"
            df = spark.read.format("sqlserver").options(**get_sql_options("raw_customer")).load()
            if id_column in df.columns:
                return sorted({str(r[id_column]) for r in df.select(id_column).distinct().collect()}, key=lambda x: int(x))
        except Exception:
            return []
        return []

    # Otherwise, read from ADLS Data Lake
    path = get_layer_path("raw", dataset)
    if not list_raw_files(dataset):
        return []
    df = (
        spark.read
             .option("header", True)
             .option("recursiveFileLookup", "true")
             .options(**ADLS_OPTS)
             .csv(path)
    )
    if id_column not in df.columns:
        return []
    return sorted(
        {str(r[id_column]) for r in df.select(id_column).distinct().collect()},
        key=lambda x: int(x)
    )

def read_existing_customers_pandas() -> "pd.DataFrame":
    empty = pd.DataFrame(
        columns=["CustomerID", "FirstName", "LastName", "Email", "Phone", "City", "State", "LastUpdated"]
    )
    
    # DYNAMIC TOGGLE
    if WRITE_CUSTOMERS_TO_SQL:
        df = read_sql_to_pandas("raw_customer")
        return df if not df.empty else empty

    # Otherwise, read ADLS
    if not list_raw_files("customers"):
        return empty
    path = get_layer_path("raw", "customers")
    sdf  = (
        spark.read
             .option("header", True)
             .option("recursiveFileLookup", "true")
             .options(**ADLS_OPTS)
             .csv(path)
    )
    return sdf.toPandas()


# ---------------------------------------------------------------------------
# Generators (identical business logic to original)
# ---------------------------------------------------------------------------

def generate_products(num_products: int, dirty_ratio: float = DIRTY_RATIO, start_id: int = 1) -> pd.DataFrame:
    rows = []
    for pid in range(start_id, start_id + num_products):
        category    = random.choice(list(CATEGORY_TREE.keys()))
        subcategory = random.choice(CATEGORY_TREE[category])
        brand       = random.choice(BRANDS)
        cost_price  = round(random.uniform(5, 1500), 2)

        if maybe(dirty_ratio):
            category = category.lower() if maybe(0.5) else category.upper()
        if maybe(dirty_ratio):
            subcategory = None
        if maybe(dirty_ratio):
            brand = None
        cost_price_str = f"{cost_price:.2f}"
        if maybe(dirty_ratio * 0.5):
            cost_price_str = f"${cost_price:.2f}" 

        rows.append({
            "ProductID":   str(pid),
            "ProductName": f"{brand or 'Generic'} {subcategory or category} {pid}",
            "Category":    category,
            "SubCategory": subcategory,
            "Brand":       brand,
            "CostPrice":   cost_price_str,
        })

    dup_count = max(1, int(num_products * 0.02))
    rows.extend(random.sample(rows, dup_count))
    return pd.DataFrame(rows).astype("string")


def generate_customers_full(num_customers: int, dirty_ratio: float = DIRTY_RATIO,
                             as_of_date: str = RUN_DATE) -> pd.DataFrame:
    as_of = datetime.strptime(as_of_date, "%Y-%m-%d")
    rows  = [_make_customer_row(cid, as_of, dirty_ratio) for cid in range(1, num_customers + 1)]
    dup_count = max(1, int(num_customers * 0.01))
    rows.extend(random.sample(rows, dup_count))
    return pd.DataFrame(rows).astype("string")


def generate_customers_incremental(existing_pool: "pd.DataFrame", num_new: int,
                                    num_updates: int, dirty_ratio: float = DIRTY_RATIO,
                                    as_of_date: str = RUN_DATE) -> pd.DataFrame:
    as_of = datetime.strptime(as_of_date, "%Y-%m-%d")
    rows  = []

    if len(existing_pool) > 0:
        update_ids = existing_pool["CustomerID"].sample(
            n=min(num_updates, len(existing_pool)),
            random_state=random.randint(0, 1_000_000),
        ).tolist()
        for cid in update_ids:
            base = existing_pool[existing_pool["CustomerID"] == cid].iloc[0].to_dict()
            base["City"]        = fake.city()
            base["State"]       = fake.state_abbr()
            base["Phone"]       = fake.phone_number()
            base["LastUpdated"] = (as_of - timedelta(minutes=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S")
            rows.append({k: base.get(k) for k in
                         ["CustomerID", "FirstName", "LastName", "Email", "Phone", "City", "State", "LastUpdated"]})

    max_existing_id = int(existing_pool["CustomerID"].astype(int).max()) if len(existing_pool) > 0 else 0
    for i in range(num_new):
        rows.append(_make_customer_row(max_existing_id + i + 1, as_of, dirty_ratio))

    return pd.DataFrame(rows).astype("string")


def _make_customer_row(cid: int, as_of: datetime, dirty_ratio: float) -> dict:
    first, last  = fake.first_name(), fake.last_name()
    email        = f"{first.lower()}.{last.lower()}{cid}@example.com"
    phone        = fake.phone_number()
    city, state  = fake.city(), fake.state_abbr()
    last_updated = as_of - timedelta(minutes=random.randint(0, 60))

    if maybe(dirty_ratio): email = None
    if maybe(dirty_ratio): phone = random.choice(["", "N/A", phone])
    if maybe(dirty_ratio): city  = None
    if maybe(dirty_ratio): state = state.lower()

    return {
        "CustomerID":  str(cid),
        "FirstName":   first,
        "LastName":    last,
        "Email":       email,
        "Phone":       phone,
        "City":        city,
        "State":       state,
        "LastUpdated": last_updated.strftime("%Y-%m-%d %H:%M:%S"),
    }


def generate_orders(num_orders: int, order_date: str, customer_id_pool: list,
                     product_id_pool: list, store_codes: list,
                     dirty_ratio: float = DIRTY_RATIO, start_order_id: int = 1) -> pd.DataFrame:
    rows = []
    for i in range(num_orders):
        oid        = start_order_id + i
        cust       = random.choice(customer_id_pool)
        prod       = random.choice(product_id_pool)
        qty        = random.randint(1, 8)
        unit_price = round(random.uniform(5, 1500), 2)
        store      = random.choice(store_codes)

        if maybe(dirty_ratio):
            cust = str(int(max(customer_id_pool, key=int)) + random.randint(1000, 9999))
        if maybe(dirty_ratio):
            prod = str(int(max(product_id_pool, key=int)) + random.randint(1000, 9999))
        if maybe(dirty_ratio):
            qty = random.choice([0, -1, qty])
        unit_price_str = f"{unit_price:.2f}"
        if maybe(dirty_ratio):
            unit_price_str = random.choice(["", unit_price_str])
        if maybe(dirty_ratio):
            store = None

        rows.append({
            "OrderID":    str(oid),
            "CustomerID": cust,
            "ProductID":  prod,
            "OrderDate":  order_date,
            "Quantity":   str(qty),
            "UnitPrice":  unit_price_str,
            "StoreCode":  store,
        })

    dup_count = max(1, int(num_orders * 0.015))
    rows.extend(random.sample(rows, dup_count))
    return pd.DataFrame(rows).astype("string")

def generate_exchange_rates_payload(rate_date: str, base_currency: str = "USD") -> dict:
    currencies = ["AUD", "BRL", "CAD", "CHF", "CNY", "EUR", "GBP", "IDR", "INR", "JPY", "MXN"]
    rates = {}
    
    for ccy in currencies:
        if maybe(0.05): 
            continue
            
        if ccy == "IDR":
            rates[ccy] = random.randint(15000, 18000)
        else:
            rates[ccy] = round(random.uniform(0.5, 90), 4)

    return {
        "amount": 1.0,
        "base": base_currency,
        "date": rate_date,
        "rates": rates
    }

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

print(f"=== Generating raw data for {RUN_DATE} (mode={GENERATION_MODE}) ===")

# ---- Products ----
print("\n[products]")
if GENERATION_MODE == "initial_seed":
    products_df = generate_products(NUM_PRODUCTS)
    write_csv_to_raw(products_df, "products", f"products_{RUN_TS}.csv")
else:
    num_new_prods = int(_SEEDS.get("num_new_products_incremental", 5))
    existing_product_ids = read_existing_ids("products", "ProductID")
    start_prod_id = max(int(x) for x in existing_product_ids) + 1 if existing_product_ids else 1
    
    path = get_layer_path("raw", "products")
    existing_products_df = (
        spark.read.option("header", True)
             .option("recursiveFileLookup", "true")
             .options(**ADLS_OPTS)
             .csv(path)
    ).toPandas()
    
    new_products_df = generate_products(num_new_prods, start_id=start_prod_id)
    full_catalog_df = pd.concat([existing_products_df, new_products_df], ignore_index=True)
    
    ts_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    write_csv_to_raw(full_catalog_df, "products", f"products_full_snapshot_{ts_suffix}.csv")

product_id_pool = read_existing_ids("products", "ProductID")
if not product_id_pool:
    raise RuntimeError("No product pool available — run with generation_mode=initial_seed at least once.")

# ---- Customers (DYNAMIC TOGGLE APPLIED) ----
print("\n[customers]")
if GENERATION_MODE == "initial_seed":
    customers_df = generate_customers_full(NUM_CUSTOMERS_SEED)
    if WRITE_CUSTOMERS_TO_SQL:
        write_pandas_to_sql(customers_df, "raw_customer", "overwrite")
    else:
        write_csv_to_raw(customers_df, "customers", f"customers_full_{RUN_TS}.csv")
else:
    existing_customers = read_existing_customers_pandas()
    customers_df       = generate_customers_incremental(
        existing_customers, NUM_NEW_CUSTOMERS_INCR, NUM_CUSTOMERS_TO_UPDATE
    )
    if WRITE_CUSTOMERS_TO_SQL:
        write_pandas_to_sql(customers_df, "raw_customer", "append")
    else:
        ts_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        write_csv_to_raw(customers_df, "customers", f"customers_incremental_{ts_suffix}.csv")

customer_id_pool = read_existing_ids("customers", "CustomerID")
if not customer_id_pool:
    raise RuntimeError("No customer pool available — run with generation_mode=initial_seed at least once.")

# ---- Orders ----
print("\n[orders]")
existing_order_ids = read_existing_ids("orders", "OrderID")
start_order_id     = (max(int(x) for x in existing_order_ids) + 1) if existing_order_ids else 100000
orders_df = generate_orders(
    NUM_ORDERS, RUN_DATE, customer_id_pool, product_id_pool, STORE_CODES,
    start_order_id=start_order_id,
)
write_csv_to_raw(orders_df, "orders", f"orders_{RUN_TS}.csv")

# ---- Exchange Rates ----
print("\n[exchange_rates]")
GENERATE_FX = _CFG["pipeline"].get("generate_mock_fx", False)

if GENERATE_FX:
    fx_payload = generate_exchange_rates_payload(RUN_DATE)
    write_json_to_raw(fx_payload, "exchange_rates", f"fx_rates_{RUN_TS}.json")
else:
    print("[skip] FX Generation disabled in config. Assuming ADF / API handles daily loads.")

print("\n=== Done ===")
for ds in ["orders", "products", "customers", "exchange_rates"]:
    if ds == "customers" and WRITE_CUSTOMERS_TO_SQL:
        print(f"       customers: Sent to Azure SQL table 'raw_customer'!")
    else:
        files = list_raw_files(ds)
        print(f"{ds:>16}: {len(files)} file(s) in {get_layer_path('raw', ds)}")