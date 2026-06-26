# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM capstone.capstone_gold_check.dim_customer limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capstone.capstone_gold_check.dim_product limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capstone.capstone_gold_check.dim_date limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capstone.capstone_gold_check.fact_sales limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capstone.capstone_gold_check.agg_daily_sales_by_store limit 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM capstone.capstone_gold_check.agg_sales_by_category limit 50;